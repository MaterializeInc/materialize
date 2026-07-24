// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An implementation of the `Correction` data structure used by the MV sink's `write_batches`
//! operator to stash updates before they are written.
//!
//! The `Correction` data structure provides methods to:
//!  * insert new updates
//!  * advance the compaction frontier (called `since`)
//!  * obtain an iterator over consolidated updates before some `upper`
//!  * force consolidation of updates before some `upper`
//!
//! The goal is to provide good performance for each of these operations, even in the presence of
//! future updates. MVs downstream of temporal filters might have to deal with large amounts of
//! retractions for future times and we want those to be handled efficiently as well.
//!
//! Note that `Correction` does not provide a method to directly remove updates. Instead updates
//! are removed by inserting their retractions so that they consolidate away to nothing.
//!
//! ## Storage of Updates
//!
//! Stored updates are of the form `(data, time, diff)`, where `time` and `diff` are fixed to
//! [`mz_repr::Timestamp`] and [`mz_repr::Diff`], respectively.
//!
//! [`CorrectionV2`] holds onto a list of `Chain`s containing `Chunk`s of stashed updates. Each
//! `Chunk` is a columnation region containing a fixed maximum number of updates. All updates in
//! a chunk, and all updates in a chain, are ordered by (time, data) and consolidated.
//!
//! Chains live in three places:
//!
//!  * A [`BucketChain`] partitions times at or beyond the `boundary` (the largest read `upper`
//!    seen so far) into buckets of exponentially growing time ranges, each holding a list of
//!    chains. Reads only touch the buckets below their `upper`, so the bulk of the buffered
//!    updates — in particular far-future retractions produced by temporal filters — is left
//!    alone.
//!  * `pending_low` holds chains at times below the `boundary`, mostly insertions arriving
//!    through the persist feedback.
//!  * `emitted` is a single chain holding the updates returned by the last read. Updates must
//!    stay in the buffer until their feedback retractions arrive, and keeping them separate from
//!    the bucket chain means reads never have to re-merge future updates.
//!
//! ```text
//!       chain[0]   |   chain[1]   |   chain[2]
//!                  |              |
//!     chunk[0]     | chunk[0]     | chunk[0]
//!       (a, 1, +1) |   (a, 1, +1) |   (d, 3, +1)
//!       (b, 1, +1) |   (b, 2, -1) |   (d, 4, -1)
//!     chunk[1]     | chunk[1]     |
//!       (c, 1, +1) |   (c, 2, -2) |
//!       (a, 2, -1) |   (c, 4, -1) |
//!     chunk[2]     |              |
//!       (b, 2, +1) |              |
//!       (c, 2, +1) |              |
//!     chunk[3]     |              |
//!       (b, 3, -1) |              |
//!       (c, 3, +1) |              |
//! ```
//!
//! The "chain invariant" states that each chain in a bucket has at least `chain_proportionality` times as
//! many updates as the next one. This means that chain sizes will often be powers of
//! `chain_proportionality`, but they don't have to be. For example, for a proportionality of 2,
//! the chain sizes `[11, 5, 2, 1]` would satisfy the chain invariant.
//!
//! Note that the invariant is maintained on update counts, not chunk counts. Chunks are
//! byte-bounded (see `ChunkBuilder`), so chunk count is not proportional to update count and
//! would be a poor proxy: any chain below the chunk byte boundary is a single chunk regardless
//! of how many updates it holds, which would let the geometric invariant collapse and break the
//! O(log N) amortization of inserts.
//!
//! Choosing the `chain_proportionality` value allows tuning the trade-off between memory and CPU
//! resources required to maintain corrections. A higher proportionality forces more frequent chain
//! merges, and therefore consolidation, reducing memory usage but increasing CPU usage.
//!
//! ## Inserting Updates
//!
//! A batch of updates is routed by time: updates below the `boundary` become a `pending_low`
//! chain, the rest is appended as new chains to their respective buckets. Appending to a bucket
//! merges chains until the chain invariant is restored.
//!
//! Inserting an update into the correction buffer can be expensive: It involves allocating a new
//! chunk, copying the update in, and then likely merging with an existing chain to restore the
//! chain invariant. If updates trickle in in small batches, this can cause a considerable
//! overhead. To amortize this overhead, new updates aren't immediately inserted into the sorted
//! chains but instead stored in a `Stage` buffer. Once enough updates have been staged to fill a
//! `Chunk`, they are sorted and routed.
//!
//! The insert operation has an amortized complexity of O(log N), with N being the current number
//! of updates stored.
//!
//! ## Retrieving Consolidated Updates
//!
//! Retrieving consolidated updates before a given `upper` works by peeling all buckets below the
//! `upper` off the bucket chain, splitting their chains, the pending low chains, and the previous
//! `emitted` chain at the `upper`, merging the parts below the `upper` into the new `emitted`
//! chain, and returning an iterator over that chain.
//!
//! Because each chain contains updates ordered by time first, splitting a chain at the `upper`
//! reuses whole chunks and copies at most one chunk straddling the split point. Updates at times
//! at or beyond the `upper` are never touched, no matter how many the buffer holds. The
//! complexity of a read is O(U log K), with U being the number of updates before `upper` and K
//! the number of chains containing them.
//!
//! ## Merging Chains
//!
//! Merging multiple chains into a single chain is done using a k-way merge. As the input chains
//! are sorted by (time, data) and consolidated, the same properties hold for the output chain. The
//! complexity of a merge of K chains containing N updates is O(N log K).
//!
//! There is a twist though: Merging also has to respect the `since` frontier, which determines how
//! far the times of updates should be advanced. Advancing times in a sorted chain of updates
//! can make them become unsorted, so we cannot just merge the chains from top to bottom.
//!
//! For example, consider these two chains, assuming `since = [2]`:
//!   chain 1: [(c, 1, +1), (b, 2, -1), (a, 3, -1)]
//!   chain 2: [(b, 1, +1), (a, 2, +1), (c, 2, -1)]
//! After time advancement, the chains look like this:
//!   chain 1: [(c, 2, +1), (b, 2, -1), (a, 3, -1)]
//!   chain 2: [(b, 2, +1), (a, 2, +1), (c, 2, -1)]
//! Merging them naively yields [(b, 2, +1), (a, 2, +1), (b, 2, -1), (a, 3, -1)], a chain that's
//! neither sorted nor consolidated.
//!
//! Times below the `since` can only exist in chains read by `consolidate_before`, and only if
//! the `since` advanced past buffered times since the previous read. For few distinct stale
//! times — the steady state, where the previously emitted chain was written just before the
//! since advanced past it — we merge sub-chains, one for each distinct time that's before or at
//! the `since`. Each of these sub-chains retains the (time, data) ordering after the time
//! advancement to `since`, so merging those yields the expected result.
//!
//! For the above example, the chains we would merge are:
//!   chain 1.a: [(c, 2, +1)]
//!   chain 1.b: [(b, 2, -1), (a, 3, -1)]
//!   chain 2.a: [(b, 2, +1)],
//!   chain 2.b: [(a, 2, +1), (c, 2, -1)]
//!
//! For many distinct stale times — e.g. a since jump across many buffered timestamps when a sink
//! restarts with an old as-of — the number of sub-chains grows with the number of distinct times,
//! so we instead materialize the affected updates, advance their times, and sort and consolidate
//! them in one O(U log U) pass.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, VecDeque};
use std::fmt;
use std::rc::Rc;
use std::sync::{Mutex, OnceLock};

use columnar::{Columnar, Index, Len, Ref};
use mz_ore::cast::CastLossy;
use mz_ore::pool::{ChunkHandle, ChunkHints, Pool};
use mz_ore::soft_assert_or_log;
use mz_persist_client::metrics::{SinkMetrics, SinkWorkerMetrics, UpdateDelta};
use mz_repr::{Diff, Timestamp};
use mz_timely_util::columnar::Column;
use mz_timely_util::columnar::chunk::LZ4_CODEC;
use mz_timely_util::pool_config;
use mz_timely_util::temporal::{Bucket, BucketChain};
use timely::PartialOrder;
use timely::dataflow::channels::ContainerBytes;
use timely::progress::Antichain;

use crate::sink::correction::{ChannelLogging, SizeMetrics};

/// Convenient alias for use in data trait bounds.
///
/// `D` is constrained to be `Columnar`, so that updates can be stored in a single columnar
/// region per chunk, and the variable-length payload (e.g. `Row` bytes) lives in the same
/// allocation as the rest of the chunk. The `Ref`-level `Eq + Ord` bounds let the merge/heap
/// code compare updates directly through the columnar borrow, avoiding `into_owned` clones
/// on the hot path.
pub trait Data:
    differential_dataflow::Data
    + Columnar<Container: Send + Sync + Clone + for<'a> columnar::Borrow<Ref<'a>: Eq + Ord>>
    + Send
    + Sync
{
}
impl<D> Data for D where
    D: differential_dataflow::Data
        + Columnar<Container: Send + Sync + Clone + for<'a> columnar::Borrow<Ref<'a>: Eq + Ord>>
        + Send
        + Sync
{
}

/// A data structure used to store corrections in the MV sink implementation.
///
/// In contrast to `CorrectionV1`, this implementation stores updates in columnation regions,
/// allowing their memory to be transparently spilled to disk.
#[derive(Debug)]
pub struct CorrectionV2<D: Data> {
    /// Bucketed storage for updates at times at or beyond `boundary`.
    ///
    /// Buckets cover exponentially growing time ranges, so reads only touch the buckets below
    /// their `upper`, and far-future updates (e.g. retractions produced by temporal filters) are
    /// rarely touched.
    chain: BucketChain<ChainBucket<D>>,
    /// Chains at times below `boundary` that were not yet emitted.
    ///
    /// Filled by inserts at times below the boundary (mostly persist feedback) and by the
    /// remainders of `emitted` when a read uses a smaller `upper` than the previous one. Merged
    /// into `emitted` by the next read.
    pending_low: Vec<Chain<D>>,
    /// Updates that were emitted by `updates_before` but not yet cancelled by persist feedback.
    ///
    /// Sorted and consolidated, with all times advanced to the `since`.
    emitted: Chain<D>,
    /// A staging area for updates, to speed up small inserts.
    stage: Stage<D>,
    /// The lower bound of times stored in `chain`. Only ever advances.
    ///
    /// Times below the boundary have been peeled off the bucket chain and can only be stored in
    /// `pending_low` or `emitted`.
    boundary: Antichain<Timestamp>,
    /// The frontier by which all contained times are advanced.
    since: Antichain<Timestamp>,

    /// Total count of updates in the correction buffer.
    ///
    /// Tracked to compute deltas in `update_metrics`.
    prev_update_count: usize,
    /// Total heap size used by the correction buffer.
    ///
    /// Tracked to compute deltas in `update_metrics`.
    prev_size: SizeMetrics,
    /// Global persist sink metrics.
    metrics: SinkMetrics,
    /// Per-worker persist sink metrics.
    worker_metrics: SinkWorkerMetrics,
    /// Introspection logging.
    logging: Option<ChannelLogging>,
}

/// Fuel for restoring the bucket chain invariant after peeling.
///
/// Bounds the restoration work per buffer operation. The bucket chain remains functional when
/// restoration is incomplete -- peeling and finding work on ill-formed chains, at the cost of
/// more in-line splitting -- so leftover restoration is simply picked up by the next operation.
///
/// `restore` spends one unit of fuel per bucket split, and a single `peel` leaves at most
/// `BucketTimestamp::DOMAIN` (64) buckets to re-split, so this budget completes restoration in one
/// call for any realistic buffer. It is deliberately generous: the "incomplete restoration is
/// picked up next op" path is a correctness safety net for pathological bucket counts, not a hot
/// path we expect to exercise. Lower it if restoration ever needs to interleave with other work.
const RESTORE_FUEL: i64 = 1_000_000;

impl<D: Data> CorrectionV2<D> {
    /// Construct a new [`CorrectionV2`] instance.
    pub fn new(
        metrics: SinkMetrics,
        worker_metrics: SinkWorkerMetrics,
        logging: Option<ChannelLogging>,
        chain_proportionality: f64,
        chunk_size: usize,
    ) -> Self {
        let update_size = std::mem::size_of::<(D, Timestamp, Diff)>();
        let chunk_capacity = std::cmp::max(chunk_size / update_size, 1);

        Self {
            chain: BucketChain::new(ChainBucket::new(chain_proportionality, logging.clone())),
            pending_low: Vec::new(),
            emitted: Chain::new(),
            stage: Stage::new(logging.clone(), chunk_capacity),
            boundary: Antichain::from_elem(Timestamp::MIN),
            since: Antichain::from_elem(Timestamp::MIN),
            prev_update_count: 0,
            prev_size: Default::default(),
            metrics,
            worker_metrics,
            logging,
        }
    }

    /// Insert a batch of updates.
    pub fn insert(&mut self, updates: &mut Vec<(D, Timestamp, Diff)>) {
        let Some(since_ts) = self.since.as_option() else {
            // If the since is the empty frontier, discard all updates.
            updates.clear();
            return;
        };

        for (_, time, _) in &mut *updates {
            *time = std::cmp::max(*time, *since_ts);
        }

        self.insert_inner(updates);
    }

    /// Insert a batch of updates, after negating their diffs.
    pub fn insert_negated(&mut self, updates: &mut Vec<(D, Timestamp, Diff)>) {
        let Some(since_ts) = self.since.as_option() else {
            // If the since is the empty frontier, discard all updates.
            updates.clear();
            return;
        };

        for (_, time, diff) in &mut *updates {
            *time = std::cmp::max(*time, *since_ts);
            *diff = -*diff;
        }

        self.insert_inner(updates);
    }

    /// Insert a batch of updates into the stage, flushing it when full.
    ///
    /// All times are expected to be >= the `since`.
    fn insert_inner(&mut self, updates: &mut Vec<(D, Timestamp, Diff)>) {
        debug_assert!(updates.iter().all(|(_, t, _)| self.since.less_equal(t)));

        if let Some(mut ready) = self.stage.insert(updates) {
            self.route(&mut ready);
        }

        self.update_metrics();
    }

    /// Route a batch of sorted, consolidated updates to `pending_low` or their chain buckets.
    fn route(&mut self, updates: &mut Vec<(D, Timestamp, Diff)>) {
        // Updates at times below the boundary become a pending low chain.
        let idx = updates.partition_point(|(_, t, _)| !self.boundary.less_equal(t));
        if idx > 0 {
            let mut builder = ChainBuilder::default();
            builder.extend(updates.drain(..idx));
            let chain = builder.finish();
            if !chain.is_empty() {
                self.log_chain_created(&chain);
                self.pending_low.push(chain);
            }
        }

        // Updates at times at or beyond the boundary go into their chain buckets. Walk ranges of
        // times that fall into the same bucket, to push batches of updates at once.
        let mut drain = updates.drain(..).peekable();
        while let Some(update) = drain.next() {
            let time = update.1;
            let range = self
                .chain
                .range_of(&time)
                .expect("bucket chain covers all times at or beyond the boundary");
            let mut builder = ChainBuilder::default();
            builder.extend(std::iter::once(update));
            while let Some(update) = drain.next_if(|(_, t, _)| range.contains(t)) {
                builder.extend(std::iter::once(update));
            }
            let bucket = self
                .chain
                .find_mut(&range.start)
                .expect("bucket chain covers all times at or beyond the boundary");
            bucket.push_chain(builder.finish());
        }
    }

    /// Return consolidated updates before the given `upper`.
    pub fn updates_before<'a>(
        &'a mut self,
        upper: &Antichain<Timestamp>,
    ) -> impl Iterator<Item = (D, Timestamp, Diff)> + Send + 'a {
        // All contained times are advanced to at least the `since`, so a read at an `upper` that
        // is not beyond the `since` is always empty. Short-circuit to avoid the eager peel, merge,
        // and `boundary` advancement that `consolidate_before` would otherwise perform. Normal
        // reads and `consolidate_at_since` always pass an `upper` beyond the `since`.
        if !PartialOrder::less_than(&self.since, upper) {
            return None.into_iter().flatten();
        }

        self.consolidate_before(upper);

        // After `consolidate_before`, `emitted` holds exactly the updates before `upper`: every
        // path that populates it splits at `upper` (pushing the remainder to `pending_low`), and
        // the guard above guarantees `upper > since`, so advancing stale times to the `since`
        // cannot lift them to or beyond `upper`. We can therefore yield all of `emitted`. Guard
        // the invariant: a violation would write updates beyond the batch upper to persist.
        soft_assert_or_log!(
            self.emitted
                .last()
                .is_none_or(|(_, t, _)| !upper.less_equal(&t)),
            "emitted contains times at or beyond the upper",
        );
        Some(self.emitted.iter()).into_iter().flatten()
    }

    /// Consolidate all updates before the given `upper` into the `emitted` chain.
    ///
    /// Once this method returns, `emitted` contains all updates at times before `upper`,
    /// consolidated. It can also contain updates at times at or beyond `upper` if `upper` is not
    /// beyond the `since`.
    fn consolidate_before(&mut self, upper: &Antichain<Timestamp>) {
        if let Some(mut ready) = self.stage.flush() {
            self.route(&mut ready);
        }

        let Some(&since_ts) = self.since.as_option() else {
            // If the since is the empty frontier, discard all updates.
            let peeled = self.chain.peel(Antichain::new().borrow());
            for bucket in peeled {
                for chain in bucket.into_chains() {
                    self.log_chain_dropped(&chain);
                }
            }
            for chain in std::mem::take(&mut self.pending_low) {
                self.log_chain_dropped(&chain);
            }
            let emitted = std::mem::replace(&mut self.emitted, Chain::new());
            if !emitted.is_empty() {
                self.log_chain_dropped(&emitted);
            }
            self.update_metrics();
            return;
        };

        // Peel the buckets below the upper off the bucket chain. Bucket splits during the peel
        // only touch chunks around the upper; chunks wholly on either side are reused.
        let peeled = self.chain.peel(upper.borrow());
        if PartialOrder::less_than(&self.boundary, upper) {
            self.boundary = upper.clone();
        }

        // Collect candidate chains: peeled bucket contents, pending low chains, and the previous
        // emitted chain. All contain only times below the boundary.
        let emitted = std::mem::replace(&mut self.emitted, Chain::new());
        let mut candidates: Vec<Chain<D>> = Vec::new();
        for bucket in peeled {
            candidates.extend(bucket.into_chains());
        }
        candidates.append(&mut self.pending_low);
        if !emitted.is_empty() {
            candidates.push(emitted);
        }

        if candidates.is_empty() {
            self.restore_chain();
            self.update_metrics();
            return;
        }

        candidates.iter().for_each(|c| self.log_chain_dropped(c));

        // Split the candidates at the upper. Parts at or beyond the upper (possible when `upper`
        // regresses below a previous one) stay pending.
        let mut lowers = Vec::new();
        for chain in candidates {
            match upper.as_option() {
                Some(&upper_ts) => {
                    let (lower, remainder) = chain.split_at_time(upper_ts);
                    if !lower.is_empty() {
                        lowers.push(lower);
                    }
                    if !remainder.is_empty() {
                        self.log_chain_created(&remainder);
                        self.pending_low.push(remainder);
                    }
                }
                // The empty upper is greater than all times.
                None => lowers.push(chain),
            }
        }

        // Merge the lower parts into the new emitted chain, advancing times below the since.
        // Advancing times in a (time, data)-sorted chain can break its sort order, so chains
        // containing stale times cannot be merged as they are. Stale times are expected in steady
        // state: the previous emitted chain was written before the since advanced past it.
        //
        // Count the distinct stale times, up to a small cap. For few distinct stale times -- the
        // steady state -- split cursors into runs that remain sorted under advancement and merge
        // those. For many distinct stale times -- e.g. a since jump across many buffered
        // timestamps when a sink restarts with an old as-of -- the number of runs and the cost of
        // cloning cursor state per run grow with the number of distinct times, so materialize,
        // advance, and consolidate in one O(U log U) pass instead.
        const MAX_STALE_RUNS: usize = 32;
        let mut stale_times = 0;
        for chain in &lowers {
            stale_times += chain.distinct_times_before(since_ts, MAX_STALE_RUNS - stale_times);
            if stale_times >= MAX_STALE_RUNS {
                break;
            }
        }

        // Emission-bound chains are depth 0: they are about to be read and
        // then cancelled by feedback retractions, the hottest data the
        // buffer holds.
        let merged = if stale_times == 0 {
            let cursors: Vec<_> = lowers.into_iter().filter_map(Chain::into_cursor).collect();
            merge_cursors(0, cursors)
        } else if stale_times < MAX_STALE_RUNS {
            let mut runs = Vec::new();
            for chain in lowers {
                if let Some(cursor) = chain.into_cursor() {
                    runs.append(&mut cursor.advance_by(since_ts));
                }
            }
            merge_cursors(0, runs)
        } else {
            let mut updates: Vec<_> = lowers.iter().flat_map(|c| c.iter()).collect();
            for (_, time, _) in &mut updates {
                *time = std::cmp::max(*time, since_ts);
            }
            consolidate(&mut updates);
            let mut builder = ChainBuilder::default();
            builder.extend(updates);
            let chain = builder.finish();

            // Advancement can move updates to or beyond the upper; such updates stay pending.
            match upper.as_option() {
                Some(&upper_ts) => {
                    let (lower, remainder) = chain.split_at_time(upper_ts);
                    if !remainder.is_empty() {
                        self.log_chain_created(&remainder);
                        self.pending_low.push(remainder);
                    }
                    lower
                }
                None => chain,
            }
        };

        if !merged.is_empty() {
            self.log_chain_created(&merged);
        }
        self.emitted = merged;

        self.restore_chain();
        self.update_metrics();
    }

    /// Perform a bounded amount of work towards restoring the bucket chain invariant.
    ///
    /// Restoration is allowed to remain incomplete: the bucket chain supports peeling and finding
    /// on ill-formed chains, so any leftover work is picked up by subsequent operations. The fuel
    /// bound keeps individual buffer operations from stalling the operator that owns the buffer.
    fn restore_chain(&mut self) {
        let mut fuel = RESTORE_FUEL;
        self.chain.restore(&mut fuel);
    }

    /// Advance the since frontier.
    ///
    /// Time advancement of updates in the bucket chain is lazy: it happens when the updates are
    /// consolidated by a read.
    ///
    /// # Panics
    ///
    /// Panics if the given `since` is less than the current since frontier.
    pub fn advance_since(&mut self, since: Antichain<Timestamp>) {
        assert!(PartialOrder::less_equal(&self.since, &since));
        self.stage.advance_times(&since);
        self.since = since;
    }

    /// Consolidate all updates at the current `since`.
    pub fn consolidate_at_since(&mut self) {
        let upper_ts = self.since.as_option().and_then(|t| t.try_step_forward());
        if let Some(upper_ts) = upper_ts {
            let upper = Antichain::from_elem(upper_ts);
            self.consolidate_before(&upper);
        }
    }

    fn log_chain_created(&self, chain: &Chain<D>) {
        if let Some(logging) = &self.logging {
            logging.chain_created(chain.update_count);
        }
    }

    fn log_chain_dropped(&self, chain: &Chain<D>) {
        if let Some(logging) = &self.logging {
            logging.chain_dropped(chain.update_count);
        }
    }

    /// Update persist sink metrics.
    fn update_metrics(&mut self) {
        let mut new_size = self.stage.get_size();
        let mut new_length = self.stage.data.len();
        for chain in &self.pending_low {
            new_size += chain.get_size();
            new_length += chain.update_count;
        }
        new_size += self.emitted.get_size();
        new_length += self.emitted.update_count;
        for bucket in self.chain.buckets() {
            for chain in &bucket.chains {
                new_size += chain.get_size();
                new_length += chain.update_count;
            }
        }

        self.update_metrics_inner(new_size, new_length);
    }

    /// Update persist sink metrics to the given new size and length.
    fn update_metrics_inner(&mut self, new_size: SizeMetrics, new_length: usize) {
        let old_size = self.prev_size;
        let old_length = self.prev_update_count;
        let len_delta = UpdateDelta::new(new_length, old_length);
        let cap_delta = UpdateDelta::new(new_size.capacity, old_size.capacity);
        self.metrics
            .report_correction_update_deltas(len_delta, cap_delta);
        self.worker_metrics
            .report_correction_update_totals(new_length, new_size.capacity);

        if let Some(logging) = &self.logging {
            let i = |x: usize| isize::try_from(x).expect("must fit");
            logging.report_size_diff(i(new_size.size) - i(old_size.size));
            logging.report_capacity_diff(i(new_size.capacity) - i(old_size.capacity));
            logging.report_allocations_diff(i(new_size.allocations) - i(old_size.allocations));
        }

        self.prev_size = new_size;
        self.prev_update_count = new_length;
    }
}

/// Merge the given cursors into one chain of the given generational depth.
fn merge_cursors<D: Data>(depth: u8, cursors: Vec<Cursor<D>>) -> Chain<D> {
    match cursors.len() {
        0 => Chain::new(),
        1 => {
            let [cur] = cursors.try_into().unwrap();
            let mut chain = cur.into_chain();
            chain.depth = depth;
            chain
        }
        2 => {
            let [a, b] = cursors.try_into().unwrap();
            merge_2(depth, a, b)
        }
        _ => merge_many(depth, cursors),
    }
}

/// Merge the given two cursors using a 2-way merge.
///
/// This function is a specialization of `merge_many` that avoids the overhead of a binary heap.
fn merge_2<D: Data>(depth: u8, cursor1: Cursor<D>, cursor2: Cursor<D>) -> Chain<D> {
    let mut rest1 = Some(cursor1);
    let mut rest2 = Some(cursor2);
    let mut merged = ChainBuilder::at_depth(depth);

    loop {
        match (rest1, rest2) {
            (Some(c1), Some(c2)) => {
                let (d1, t1, r1) = c1.get();
                let (d2, t2, r2) = c2.get();

                match (t1, d1).cmp(&(t2, d2)) {
                    Ordering::Less => {
                        merged.push_ref((d1, t1, r1));
                        rest1 = c1.step();
                        rest2 = Some(c2);
                    }
                    Ordering::Greater => {
                        merged.push_ref((d2, t2, r2));
                        rest1 = Some(c1);
                        rest2 = c2.step();
                    }
                    Ordering::Equal => {
                        let r = r1 + r2;
                        if r != Diff::ZERO {
                            merged.push_ref((d1, t1, r));
                        }
                        rest1 = c1.step();
                        rest2 = c2.step();
                    }
                }
            }
            (Some(c), None) | (None, Some(c)) => {
                merged.push_cursor(c);
                break;
            }
            (None, None) => break,
        }
    }

    merged.finish()
}

/// Merge the given cursors using a k-way merge with a binary heap.
fn merge_many<D: Data>(depth: u8, cursors: Vec<Cursor<D>>) -> Chain<D> {
    let mut heap = MergeHeap::from_iter(cursors);
    let mut merged = ChainBuilder::at_depth(depth);
    while let Some(cursor1) = heap.pop() {
        let (data, time, mut diff) = cursor1.get();

        while let Some((cursor2, r)) = heap.pop_equal(data, time) {
            diff += r;
            if let Some(cursor2) = cursor2.step() {
                heap.push(cursor2);
            }
        }

        if diff != Diff::ZERO {
            merged.push_ref((data, time, diff));
        }
        if let Some(cursor1) = cursor1.step() {
            heap.push(cursor1);
        }
    }

    merged.finish()
}

impl<D: Data> Drop for CorrectionV2<D> {
    fn drop(&mut self) {
        for bucket in self.chain.buckets() {
            bucket.chains.iter().for_each(|c| self.log_chain_dropped(c));
        }
        self.pending_low
            .iter()
            .for_each(|c| self.log_chain_dropped(c));
        if !self.emitted.is_empty() {
            self.log_chain_dropped(&self.emitted);
        }
        self.update_metrics_inner(Default::default(), 0);
    }
}

/// A bucket of `Chain`s, for use in a [`BucketChain`].
///
/// All chains are individually sorted by (time, data) and consolidated, but updates can appear in
/// multiple chains, so consumers must merge the chains to obtain consolidated updates.
struct ChainBucket<D: Data> {
    /// The contained chains.
    ///
    /// Maintained with the chain invariant on pushes; splits can leave it violated until the next
    /// push restores it.
    chains: Vec<Chain<D>>,
    /// The size factor of subsequent chains required by the chain invariant.
    chain_proportionality: f64,
    /// Introspection logging.
    logging: Option<ChannelLogging>,
}

impl<D: Data> fmt::Debug for ChainBucket<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChainBucket")
            .field("chains", &self.chains)
            .finish_non_exhaustive()
    }
}

impl<D: Data> ChainBucket<D> {
    /// Construct a new, empty `ChainBucket`.
    fn new(chain_proportionality: f64, logging: Option<ChannelLogging>) -> Self {
        Self {
            chains: Vec::new(),
            chain_proportionality,
            logging,
        }
    }

    /// Push a chain onto the bucket, restoring the chain invariant.
    fn push_chain(&mut self, chain: Chain<D>) {
        if chain.is_empty() {
            return;
        }
        if let Some(logging) = &self.logging {
            logging.chain_created(chain.update_count);
        }
        self.chains.push(chain);

        // Restore the chain invariant.
        let prop = self.chain_proportionality;
        let merge_needed = |chains: &[Chain<_>]| match chains {
            [.., prev, last] => {
                let last_len = f64::cast_lossy(last.update_count);
                let prev_len = f64::cast_lossy(prev.update_count);
                last_len * prop > prev_len
            }
            _ => false,
        };

        while merge_needed(&self.chains) {
            let a = self.chains.pop().unwrap();
            let b = self.chains.pop().unwrap();
            if let Some(logging) = &self.logging {
                logging.chain_dropped(a.update_count);
                logging.chain_dropped(b.update_count);
            }

            // Generation counting: the merged chain is one generation past
            // its deepest input, so repeatedly merged (older, colder) data
            // sinks into deeper pool bands.
            let depth = a.depth.max(b.depth).saturating_add(1);
            let cursors = [a, b].into_iter().filter_map(Chain::into_cursor).collect();
            let merged = merge_cursors(depth, cursors);
            if !merged.is_empty() {
                if let Some(logging) = &self.logging {
                    logging.chain_created(merged.update_count);
                }
                self.chains.push(merged);
            }
        }
    }

    /// Convert the bucket into its contained chains.
    fn into_chains(self) -> Vec<Chain<D>> {
        self.chains
    }
}

impl<D: Data> Bucket for ChainBucket<D> {
    type Timestamp = Timestamp;

    fn split(self, timestamp: &Self::Timestamp, fuel: &mut i64) -> (Self, Self) {
        let mut lower = Self::new(self.chain_proportionality, self.logging.clone());
        let mut upper = Self::new(self.chain_proportionality, self.logging.clone());

        for chain in self.chains {
            // Whole chunks are reused; at most one chunk straddling the timestamp is copied per
            // chain. Account fuel at chunk granularity.
            *fuel = fuel.saturating_sub(i64::try_from(chain.chunks.len()).expect("must fit"));

            if let Some(logging) = &self.logging {
                logging.chain_dropped(chain.update_count);
            }
            let (lo, hi) = chain.split_at_time(*timestamp);
            for (part, target) in [(lo, &mut lower), (hi, &mut upper)] {
                if !part.is_empty() {
                    if let Some(logging) = &self.logging {
                        logging.chain_created(part.update_count);
                    }
                    target.chains.push(part);
                }
            }
        }

        (lower, upper)
    }
}

/// A chain of [`Chunk`]s containing updates.
///
/// All updates in a chain are sorted by (time, data) and consolidated.
///
/// Note that, in contrast to [`Chunk`]s, chains can be empty. Though we generally try to avoid
/// keeping around empty chains.
#[derive(Debug)]
struct Chain<D: Data> {
    /// The contained chunks.
    chunks: Vec<Chunk<D>>,
    /// The number of updates contained in all chunks.
    update_count: usize,
    /// The chain's generational depth: 0 for chains built from staged input
    /// or destined for emission, and one past the deepest input for chains
    /// built by merging. Forwarded to the buffer pool as the
    /// [`ChunkHints`] depth of chunks minted for this chain, marking
    /// older-generation data as the colder eviction and write-behind
    /// candidate.
    depth: u8,
}

impl<D: Data> Chain<D> {
    /// Construct an empty chain of depth 0.
    fn new() -> Self {
        Self {
            chunks: Default::default(),
            update_count: 0,
            depth: 0,
        }
    }

    /// Return whether the chain is empty.
    fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    /// Push a chunk onto the chain.
    ///
    /// All updates in the chunk must sort after all updates already in the chain, in
    /// (time, data)-order, to ensure the chain remains sorted.
    fn push_chunk(&mut self, chunk: Chunk<D>) {
        debug_assert!(self.can_accept_chunk(&chunk));

        self.update_count += chunk.len();
        self.chunks.push(chunk);
    }

    /// Return whether the chain can accept the given chunk at its end while preserving
    /// (time, data)-order.
    ///
    /// Uses the cached boundary times and only materializes the boundary chunks when the times
    /// tie (a single timestamp straddling the chunk boundary), so the common
    /// strictly-increasing-time case checks the invariant without paging chunks in.
    fn can_accept_chunk(&self, chunk: &Chunk<D>) -> bool {
        match self.chunks.last() {
            None => true,
            Some(last) => match last.last_time().cmp(&chunk.first_time()) {
                Ordering::Less => true,
                Ordering::Greater => false,
                Ordering::Equal => {
                    let (dc, _, _) = last.last();
                    let (d, _, _) = chunk.first();
                    dc < d
                }
            },
        }
    }

    /// Return the last update in the chain, if any.
    fn last(&self) -> Option<Ref<'_, (D, Timestamp, Diff)>> {
        self.chunks.last().map(|c| c.last())
    }

    /// Convert the chain into a cursor over the contained updates.
    fn into_cursor(self) -> Option<Cursor<D>> {
        let chunks = self.chunks.into_iter().map(Rc::new).collect();
        Cursor::new(chunks)
    }

    /// Return an iterator over the contained updates.
    fn iter(&self) -> impl Iterator<Item = (D, Timestamp, Diff)> + '_ {
        self.chunks.iter().flat_map(|c| {
            (0..c.len()).map(move |i| {
                let (d, t, r) = c.index(i);
                (D::into_owned(d), t, r)
            })
        })
    }

    /// Count the distinct times of updates at times before `time`, up to the given cap.
    ///
    /// The scan uses one binary search per distinct time, so its cost is bounded by
    /// O(cap log chunks).
    fn distinct_times_before(&self, time: Timestamp, cap: usize) -> usize {
        let mut count = 0;
        let mut chunk_idx = 0;
        let mut offset = 0;
        while count < cap && chunk_idx < self.chunks.len() {
            let chunk = &self.chunks[chunk_idx];
            let current = chunk.index(offset).1;
            if current >= time {
                break;
            }
            count += 1;
            // Skip to the first update at a time greater than `current`.
            match chunk.find_time_greater_than(current) {
                Some(idx) => offset = idx,
                None => {
                    // All later updates at `current` are in subsequent chunks.
                    chunk_idx += 1;
                    offset = 0;
                    while chunk_idx < self.chunks.len() {
                        match self.chunks[chunk_idx].find_time_greater_than(current) {
                            Some(idx) => {
                                offset = idx;
                                break;
                            }
                            None => chunk_idx += 1,
                        }
                    }
                }
            }
        }
        count
    }

    /// Split the chain at the given time.
    ///
    /// Returns two chains, the first containing all updates at times < `time`, the second
    /// containing all updates at times >= `time`. Chunks fully on either side of `time` are
    /// reused; only a chunk straddling `time` is copied.
    fn split_at_time(mut self, time: Timestamp) -> (Self, Self) {
        // Both halves inherit the source's generation: a split reorganizes
        // a chain, it does not merge one.
        let depth = self.depth;
        let mut lower = Self::new();
        let mut upper = Self::new();
        lower.depth = depth;
        upper.depth = depth;

        let Some(skip_ts) = time.step_back() else {
            // Nothing sorts before `time`.
            return (lower, self);
        };

        for chunk in self.chunks.drain(..) {
            // Route whole chunks by cached boundary times so a chunk that lands entirely on one
            // side is moved without paging it in; only a straddling chunk is materialized.
            if chunk.last_time() < time {
                lower.push_chunk(chunk);
            } else if chunk.first_time() >= time {
                upper.push_chunk(chunk);
            } else {
                // The chunk straddles `time`; copy its two halves.
                let idx = chunk
                    .find_time_greater_than(skip_ts)
                    .expect("straddles time");
                let mut builder = ChainBuilder::at_depth(depth);
                for i in 0..idx {
                    builder.push_ref(chunk.index(i));
                }
                for part in builder.finish().chunks {
                    lower.push_chunk(part);
                }
                let mut builder = ChainBuilder::at_depth(depth);
                for i in idx..chunk.len() {
                    builder.push_ref(chunk.index(i));
                }
                for part in builder.finish().chunks {
                    upper.push_chunk(part);
                }
            }
        }

        (lower, upper)
    }

    /// Return the size of the chain, for use in metrics.
    fn get_size(&self) -> SizeMetrics {
        let mut metrics = SizeMetrics::default();
        for chunk in &self.chunks {
            metrics += chunk.get_size();
        }
        metrics
    }
}

/// A builder that constructs a [`Chain`] from a stream of updates.
///
/// Wraps a [`ChunkBuilder`] and drains its minted chunks into a [`Chain`]. Pushed updates must
/// arrive in (time, data) sorted order.
struct ChainBuilder<D: Data> {
    builder: ChunkBuilder<D>,
    chain: Chain<D>,
}

impl<D: Data> Default for ChainBuilder<D> {
    fn default() -> Self {
        Self {
            builder: Default::default(),
            chain: Chain::new(),
        }
    }
}

impl<D: Data> ChainBuilder<D> {
    /// A builder whose chain, and every chunk minted for it, carries the
    /// given generational depth.
    fn at_depth(depth: u8) -> Self {
        let mut chain = Chain::new();
        chain.depth = depth;
        Self {
            builder: ChunkBuilder::at_depth(depth),
            chain,
        }
    }

    /// Push a reference-form update into the builder.
    fn push_ref(&mut self, update: Ref<'_, (D, Timestamp, Diff)>) {
        self.builder.push(update);
        self.drain();
    }

    /// Push an owned-form update into the builder.
    fn push_owned(&mut self, update: &(D, Timestamp, Diff)) {
        self.builder.push(update);
        self.drain();
    }

    /// Push the updates produced by a cursor into the builder.
    fn push_cursor(&mut self, cursor: Cursor<D>) {
        let mut rest = Some(cursor);
        while let Some(cursor) = rest.take() {
            let update = cursor.get();
            self.push_ref(update);
            rest = cursor.step();
        }
    }

    /// Move any minted chunks from the builder into the chain.
    fn drain(&mut self) {
        while let Some(chunk) = self.builder.pop() {
            self.chain.push_chunk(chunk);
        }
    }

    /// Finish building, returning the assembled [`Chain`].
    fn finish(self) -> Chain<D> {
        let Self { builder, mut chain } = self;
        for chunk in builder.finish() {
            if chunk.len() > 0 {
                chain.push_chunk(chunk);
            }
        }
        chain
    }
}

impl<D: Data> Extend<(D, Timestamp, Diff)> for ChainBuilder<D> {
    fn extend<I: IntoIterator<Item = (D, Timestamp, Diff)>>(&mut self, iter: I) {
        for update in iter {
            self.push_owned(&update);
        }
    }
}

/// A cursor over updates in a chain.
///
/// A cursor provides two guarantees:
///  * Produced updates are ordered and consolidated.
///  * A cursor always yields at least one update.
///
/// The second guarantee is enforced through the type system: Every method that steps a cursor
/// forward consumes `self` and returns an `Option<Cursor>` that's `None` if the operation stepped
/// over the last update.
///
/// A cursor holds on to `Rc<Chunk>`s, allowing multiple cursors to produce updates from the same
/// chunks concurrently. As soon as a cursor is done producing updates from a [`Chunk`] it drops
/// its reference. Once the last cursor is done with a [`Chunk`] its memory can be reclaimed.
#[derive(Clone, Debug)]
struct Cursor<D: Data> {
    /// The chunks from which updates can still be produced.
    chunks: VecDeque<Rc<Chunk<D>>>,
    /// The current offset into `chunks.front()`.
    chunk_offset: usize,
    /// An optional limit for the number of updates the cursor will produce.
    limit: Option<usize>,
    /// An optional overwrite for the timestamp of produced updates.
    overwrite_ts: Option<Timestamp>,
}

impl<D: Data> Cursor<D> {
    /// Construct a cursor over a list of chunks.
    ///
    /// Returns `None` if `chunks` is empty.
    fn new(chunks: VecDeque<Rc<Chunk<D>>>) -> Option<Self> {
        if chunks.is_empty() {
            return None;
        }

        Some(Self {
            chunks,
            chunk_offset: 0,
            limit: None,
            overwrite_ts: None,
        })
    }

    /// Set a limit for the number of updates this cursor will produce.
    ///
    /// # Panics
    ///
    /// Panics if there is already a limit lower than the new one.
    fn set_limit(mut self, limit: usize) -> Option<Self> {
        assert!(self.limit.is_none_or(|l| l >= limit));

        if limit == 0 {
            return None;
        }

        // Release chunks made unreachable by the limit.
        let mut count = 0;
        let mut idx = 0;
        let mut offset = self.chunk_offset;
        while idx < self.chunks.len() && count < limit {
            let chunk = &self.chunks[idx];
            count += chunk.len() - offset;
            idx += 1;
            offset = 0;
        }
        self.chunks.truncate(idx);

        if count > limit {
            self.limit = Some(limit);
        }

        Some(self)
    }

    /// Get a reference to the current update.
    fn get(&self) -> Ref<'_, (D, Timestamp, Diff)> {
        let chunk = self.get_chunk();
        let (d, t, r) = chunk.index(self.chunk_offset);
        let t = self.overwrite_ts.unwrap_or(t);
        (d, t, r)
    }

    /// Get a reference to the current chunk.
    fn get_chunk(&self) -> &Chunk<D> {
        &self.chunks[0]
    }

    /// Step to the next update.
    ///
    /// Returns the stepped cursor, or `None` if the step was over the last update.
    fn step(mut self) -> Option<Self> {
        if self.chunk_offset == self.get_chunk().len() - 1 {
            return self.skip_chunk().map(|(c, _)| c);
        }

        self.chunk_offset += 1;

        if let Some(limit) = &mut self.limit {
            *limit -= 1;
            if *limit == 0 {
                return None;
            }
        }

        Some(self)
    }

    /// Skip the remainder of the current chunk.
    ///
    /// Returns the forwarded cursor and the number of updates skipped, or `None` if no chunks are
    /// left after the skip.
    fn skip_chunk(mut self) -> Option<(Self, usize)> {
        let chunk = self.chunks.pop_front().expect("cursor invariant");

        if self.chunks.is_empty() {
            return None;
        }

        let skipped = chunk.len() - self.chunk_offset;
        self.chunk_offset = 0;

        if let Some(limit) = &mut self.limit {
            if skipped >= *limit {
                return None;
            }
            *limit -= skipped;
        }

        Some((self, skipped))
    }

    /// Skip all updates with times <= the given time.
    ///
    /// Returns the forwarded cursor and the number of updates skipped, or `None` if no updates are
    /// left after the skip.
    fn skip_time(mut self, time: Timestamp) -> Option<(Self, usize)> {
        if self.overwrite_ts.is_some_and(|ts| ts <= time) {
            return None;
        } else if self.get().1 > time {
            return Some((self, 0));
        }

        let mut skipped = 0;

        let new_offset = loop {
            let chunk = self.get_chunk();
            if let Some(index) = chunk.find_time_greater_than(time) {
                break index;
            }

            let (cursor, count) = self.skip_chunk()?;
            self = cursor;
            skipped += count;
        };

        skipped += new_offset - self.chunk_offset;
        self.chunk_offset = new_offset;

        Some((self, skipped))
    }

    /// Advance all updates in this cursor by the given `since_ts`.
    ///
    /// Returns a list of cursors, each of which yields ordered and consolidated updates that have
    /// been advanced by `since_ts`.
    fn advance_by(mut self, since_ts: Timestamp) -> Vec<Self> {
        // If the cursor has an `overwrite_ts`, all its updates are at the same time already. We
        // only need to advance the `overwrite_ts` by the `since_ts`.
        if let Some(ts) = self.overwrite_ts {
            if ts < since_ts {
                self.overwrite_ts = Some(since_ts);
            }
            return vec![self];
        }

        // Otherwise we need to split the cursor so that each new cursor only yields runs of
        // updates that are correctly (time, data)-ordered when advanced by `since_ts`. We achieve
        // this by splitting the cursor at each time <= `since_ts`.
        let mut splits = Vec::new();
        let mut remaining = Some(self);

        while let Some(cursor) = remaining.take() {
            let (_, time, _) = cursor.get();
            if time >= since_ts {
                splits.push(cursor);
                break;
            }

            let mut current = cursor.clone();
            if let Some((cursor, skipped)) = cursor.skip_time(time) {
                remaining = Some(cursor);
                current = current.set_limit(skipped).expect("skipped at least 1");
            }
            current.overwrite_ts = Some(since_ts);
            splits.push(current);
        }

        splits
    }

    /// Drain the cursor into a [`Chain`].
    ///
    /// This reuses the underlying chunks if possible, and writes new ones otherwise.
    fn into_chain(self) -> Chain<D> {
        match self.try_unwrap() {
            Ok(chain) => chain,
            Err((_, cursor)) => {
                let mut builder = ChainBuilder::default();
                builder.push_cursor(cursor);
                builder.finish()
            }
        }
    }

    /// Attempt to unwrap the cursor into a [`Chain`].
    ///
    /// This operation efficiently reuses chunks by directly inserting them into the output chain
    /// where possible.
    ///
    /// An unwrap is only successful if the cursor's `limit` and `overwrite_ts` are both `None` and
    /// the cursor has unique references to its chunks. If the unwrap fails, this method returns an
    /// `Err` containing the cursor in an unchanged state, allowing the caller to convert it into a
    /// chain by copying chunks rather than reusing them.
    fn try_unwrap(self) -> Result<Chain<D>, (&'static str, Self)> {
        if self.limit.is_some() {
            return Err(("cursor with limit", self));
        }
        if self.overwrite_ts.is_some() {
            return Err(("cursor with overwrite_ts", self));
        }
        if self.chunks.iter().any(|c| Rc::strong_count(c) != 1) {
            return Err(("cursor on shared chunks", self));
        }

        let mut builder = ChainBuilder::default();
        let mut remaining = Some(self);

        // We might be partway through the first chunk, in which case we can't reuse it but need to
        // allocate a new one to contain only the updates the cursor can still yield.
        while let Some(cursor) = remaining.take() {
            if cursor.chunk_offset == 0 {
                remaining = Some(cursor);
                break;
            }
            let update = cursor.get();
            builder.push_ref(update);
            remaining = cursor.step();
        }

        let mut chain = builder.finish();
        if let Some(cursor) = remaining {
            for chunk in cursor.chunks {
                let chunk = Rc::into_inner(chunk).expect("checked above");
                chain.push_chunk(chunk);
            }
        }

        Ok(chain)
    }
}

/// Bodies smaller than this stay resident: the pool's smallest size class is 64 KiB, so
/// spilling below it trades no meaningful memory for slot waste.
const SPILL_MIN_BYTES: usize = 64 << 10;

/// Serialize a column into a pool slot. The `Align` variant is already the serialized form and
/// copies in directly; other variants write their [`ContainerBytes`] encoding through a cursor
/// over the slot memory. Sizing is exact, so a short or overlong write is a contract violation
/// and panics.
fn spill_column<C: Columnar>(
    column: Column<C>,
    pool: &Pool,
    len_bytes: usize,
    hints: ChunkHints,
) -> ChunkHandle {
    debug_assert_eq!(len_bytes % 8, 0);
    match column {
        Column::Align(words) => pool.insert_with(words.len(), hints, &LZ4_CODEC, |dst| {
            dst.copy_from_slice(&words)
        }),
        other => pool.insert_with(len_bytes / 8, hints, &LZ4_CODEC, |dst| {
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

/// A non-empty chunk of updates, backed by a columnar region.
///
/// All updates in a chunk are sorted by (time, data) and consolidated.
///
/// Chunks are immutable once created. They are produced by [`ChunkBuilder`], which mints a
/// new chunk whenever its in-progress columnar container reaches a fixed serialized byte
/// boundary (~2 MiB, matching the ship granularity used elsewhere in the codebase), so each
/// chunk corresponds to a single, predictably sized allocation.
struct Chunk<D: Data> {
    /// The spilled form: the serialized column in the process buffer pool, present until the
    /// chunk is materialized.
    ///
    /// A `Mutex` (not `RefCell`) keeps the chunk `Sync`: cursors hold chunks behind a shared
    /// `Rc`, and the iterator returned by [`CorrectionV2::updates_before`] borrows them across
    /// the persist writer's `await`, so `&Chunk` must be `Send`. The lock is taken once, at
    /// materialization, and is otherwise uncontended (the sink runs single-threaded per worker).
    spilled: Mutex<Option<ChunkHandle>>,
    /// The materialized form, populated at construction when the chunk does not spill, or lazily
    /// by [`Chunk::column`] on first access.
    ///
    /// An `OnceLock` (not `OnceCell`) for the same `Sync` reason. Once set the slot is never
    /// cleared, so its address is stable and [`Chunk::index`] can hand out `Ref<'_>` borrows tied
    /// to `&self`. The allocation is freed when the chunk drops, which bounds resident memory to
    /// the chunks under an active merge front.
    resident: OnceLock<Column<(D, Timestamp, Diff)>>,
    /// Number of updates, cached so `len` and chain bookkeeping never page the chunk in.
    len: usize,
    /// Time of the first update, cached so boundary checks (`split_at_time`, `can_accept`) route
    /// a resting chunk without materializing it.
    first_time: Timestamp,
    /// Time of the last update, cached likewise.
    last_time: Timestamp,
}

impl<D: Data> fmt::Debug for Chunk<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Chunk(<{}>)", self.len())
    }
}

impl<D: Data> Chunk<D> {
    /// Wrap the given non-empty column in a chunk.
    ///
    /// Reads the cached metadata (length, boundary times) while the column is still resident,
    /// then spills the body to the process buffer pool when pool mode is active and the body is
    /// worth a slot; a spilled chunk materializes lazily on first read.
    ///
    /// # Panics
    ///
    /// Panics if the column is empty. Chunks are non-empty by construction; [`ChunkBuilder`] only
    /// ever builds a chunk from a populated column.
    fn from_column(data: Column<(D, Timestamp, Diff)>, depth: u8) -> Self {
        let (len, first_time, last_time) = {
            let borrowed = data.borrow();
            let len = borrowed.len();
            assert!(len > 0, "chunks are non-empty");
            (len, borrowed.get(0).1, borrowed.get(len - 1).1)
        };

        let mut spilled = None;
        let mut resident = OnceLock::new();
        let len_bytes = data.length_in_bytes();
        match pool_config::active_pool() {
            Some(pool) if len_bytes >= SPILL_MIN_BYTES => {
                let hints = ChunkHints { depth };
                spilled = Some(spill_column(data, &pool, len_bytes, hints));
            }
            _ => resident = OnceLock::from(data),
        }

        Self {
            spilled: Mutex::new(spilled),
            resident,
            len,
            first_time,
            last_time,
        }
    }

    /// Materialize the chunk's column, reading it back from the pool on first access.
    ///
    /// The returned reference is valid for as long as `&self`: the `OnceLock` slot is never
    /// cleared once populated, so its contents have a stable address.
    fn column(&self) -> &Column<(D, Timestamp, Diff)> {
        self.resident.get_or_init(|| {
            let handle = self
                .spilled
                .lock()
                .expect("spill mutex poisoned")
                .take()
                .expect("spilled form present until materialized");
            let mut words = Vec::new();
            handle.read_into(&mut words);
            drop(handle);
            Column::Align(words)
        })
    }

    /// Return the number of updates in the chunk.
    fn len(&self) -> usize {
        self.len
    }

    /// Return the update at the given index, paging the chunk in if necessary.
    ///
    /// # Panics
    ///
    /// Panics if the given index is not populated.
    fn index(&self, idx: usize) -> Ref<'_, (D, Timestamp, Diff)> {
        self.column().borrow().get(idx)
    }

    /// Return the first update in the chunk, paging the chunk in if necessary.
    fn first(&self) -> Ref<'_, (D, Timestamp, Diff)> {
        self.index(0)
    }

    /// Return the last update in the chunk, paging the chunk in if necessary.
    fn last(&self) -> Ref<'_, (D, Timestamp, Diff)> {
        self.index(self.len - 1)
    }

    /// Return the time of the first update, without materializing the chunk.
    fn first_time(&self) -> Timestamp {
        self.first_time
    }

    /// Return the time of the last update, without materializing the chunk.
    fn last_time(&self) -> Timestamp {
        self.last_time
    }

    /// Return the index of the first update at a time greater than `time`, or `None` if no such
    /// update exists.
    ///
    /// The early-out uses the cached last time, so a chunk whose updates are all at or before
    /// `time` is skipped without paging it in.
    fn find_time_greater_than(&self, time: Timestamp) -> Option<usize> {
        if self.last_time <= time {
            return None;
        }

        let mut lower = 0;
        let mut upper = self.len;
        while lower < upper {
            let idx = (lower + upper) / 2;
            if self.index(idx).1 > time {
                upper = idx;
            } else {
                lower = idx + 1;
            }
        }

        Some(lower)
    }

    /// Return the size of the chunk, for use in metrics.
    ///
    /// Reports resident bytes only: a spilled body lives in the process buffer pool, which does
    /// its own residency accounting, so it contributes nothing here.
    fn get_size(&self) -> SizeMetrics {
        match self.resident.get() {
            Some(col) => {
                let bytes = col.length_in_bytes();
                SizeMetrics {
                    size: bytes,
                    capacity: bytes,
                    allocations: 1,
                }
            }
            None => SizeMetrics::default(),
        }
    }
}

/// Builder that produces a stream of fixed-size [`Chunk`]s.
///
/// Wraps [`mz_timely_util::columnar::builder::ColumnBuilder`], which mints a new
/// [`Column::Align`] chunk whenever its in-progress columnar container reaches a fixed
/// serialized byte boundary (~2 MiB, matching the ship granularity used elsewhere in the
/// codebase). Each minted chunk is therefore a single, predictably-sized aligned allocation.
struct ChunkBuilder<D: Data> {
    inner: mz_timely_util::columnar::builder::ColumnBuilder<(D, Timestamp, Diff)>,
    /// The generational depth minted chunks carry as their pool hint.
    depth: u8,
}

impl<D: Data> Default for ChunkBuilder<D> {
    fn default() -> Self {
        ChunkBuilder::at_depth(0)
    }
}

impl<D: Data> ChunkBuilder<D> {
    /// A builder minting chunks at the given generational depth.
    fn at_depth(depth: u8) -> Self {
        Self {
            inner: Default::default(),
            depth,
        }
    }

    /// Push an update into the builder.
    ///
    /// Accepts whatever the inner [`ColumnBuilder`]'s [`PushInto`] impl accepts — both the
    /// `Ref<'_, (D, T, R)>` refs produced by cursors and `&(D, T, R)` references to owned
    /// tuples drained from the staging buffer.
    ///
    /// [`ColumnBuilder`]: mz_timely_util::columnar::builder::ColumnBuilder
    /// [`PushInto`]: timely::container::PushInto
    #[inline]
    fn push<T>(&mut self, item: T)
    where
        mz_timely_util::columnar::builder::ColumnBuilder<(D, Timestamp, Diff)>:
            timely::container::PushInto<T>,
    {
        timely::container::PushInto::push_into(&mut self.inner, item);
    }

    /// Pop a finished chunk, if one is available.
    fn pop(&mut self) -> Option<Chunk<D>> {
        use timely::container::ContainerBuilder;
        // `ColumnBuilder::extract` stashes the popped chunk in its `finished` slot so the
        // caller can read it through `&mut`; move it out with `mem::take` so we own it
        // (leaves `Column::Typed(Default::default())` behind, which the next `extract`
        // overwrites).
        let depth = self.depth;
        self.inner
            .extract()
            .map(|c| Chunk::from_column(std::mem::take(c), depth))
    }

    /// Finalize the builder: flush any in-progress updates as a typed chunk and drain pending.
    fn finish(mut self) -> impl Iterator<Item = Chunk<D>> {
        use timely::container::ContainerBuilder;
        // `ColumnBuilder::finish` flushes the in-progress container into the pending queue
        // (as `Column::Typed`) and returns the first pending entry. Subsequent calls drain
        // the rest until `None`. Translate that into an owning iterator.
        //
        // `finish` can hand back an empty column (e.g. when the last shipped chunk landed exactly
        // on the boundary). Skip those: `Chunk::from_column` requires a non-empty column, and an
        // empty chunk would needlessly engage the pool.
        std::iter::from_fn(move || {
            loop {
                let col = std::mem::take(self.inner.finish()?);
                if col.borrow().len() > 0 {
                    return Some(Chunk::from_column(col, self.depth));
                }
            }
        })
    }
}

/// A buffer for staging updates before they are inserted into the sorted chains.
#[derive(Debug)]
struct Stage<D> {
    /// The contained updates.
    ///
    /// This vector has a fixed capacity equal to the [`Chunk`] capacity.
    data: Vec<(D, Timestamp, Diff)>,
    /// Introspection logging.
    ///
    /// We want to report the number of records in the stage. To do so, we pretend that the stage
    /// is a chain, and every time the number of updates inside changes, the chain gets dropped and
    /// re-created.
    logging: Option<ChannelLogging>,
}

impl<D: Data> Stage<D> {
    fn new(logging: Option<ChannelLogging>, chunk_capacity: usize) -> Self {
        // For logging, we pretend the stage consists of a single chain.
        if let Some(logging) = &logging {
            logging.chain_created(0);
        }

        Self {
            data: Vec::with_capacity(chunk_capacity),
            logging,
        }
    }

    /// Insert a batch of updates, possibly producing a batch of sorted, consolidated updates
    /// ready to be stored.
    fn insert(
        &mut self,
        updates: &mut Vec<(D, Timestamp, Diff)>,
    ) -> Option<Vec<(D, Timestamp, Diff)>> {
        if updates.is_empty() {
            return None;
        }

        let prev_length = self.ilen();

        // Determine how many chunks we can fill with the available updates.
        let update_count = self.data.len() + updates.len();
        let chunk_capacity = self.data.capacity();
        let chunk_count = update_count / chunk_capacity;

        let mut new_updates = updates.drain(..);

        // If we have enough shipable updates, collect them and consolidate.
        let maybe_ready = if chunk_count > 0 {
            let ship_count = chunk_count * chunk_capacity;
            let mut buffer = Vec::with_capacity(ship_count);

            buffer.append(&mut self.data);
            while buffer.len() < ship_count {
                let update = new_updates.next().unwrap();
                buffer.push(update);
            }

            consolidate(&mut buffer);

            Some(buffer)
        } else {
            None
        };

        // Stage the remaining updates.
        Extend::extend(&mut self.data, new_updates);

        self.log_length_diff(self.ilen() - prev_length);

        maybe_ready
    }

    /// Flush all currently staged updates, returning them sorted and consolidated.
    fn flush(&mut self) -> Option<Vec<(D, Timestamp, Diff)>> {
        self.log_length_diff(-self.ilen());

        consolidate(&mut self.data);

        if self.data.is_empty() {
            return None;
        }

        let capacity = self.data.capacity();
        let data = std::mem::replace(&mut self.data, Vec::with_capacity(capacity));
        Some(data)
    }

    /// Advance the times of staged updates by the given `since`.
    fn advance_times(&mut self, since: &Antichain<Timestamp>) {
        let Some(since_ts) = since.as_option() else {
            // If the since is the empty frontier, discard all updates.
            self.log_length_diff(-self.ilen());
            self.data.clear();
            return;
        };

        for (_, time, _) in &mut self.data {
            *time = std::cmp::max(*time, *since_ts);
        }
    }

    /// Return the size of the stage, for use in metrics.
    ///
    /// Note: We don't follow pointers here, so the returned `size` and `capacity` values are
    /// under-estimates. That's fine as the stage should always be small.
    fn get_size(&self) -> SizeMetrics {
        SizeMetrics {
            size: self.data.len() * std::mem::size_of::<(D, Timestamp, Diff)>(),
            capacity: self.data.capacity() * std::mem::size_of::<(D, Timestamp, Diff)>(),
            allocations: 1,
        }
    }

    /// Return the number of updates in the stage, as an `isize`.
    fn ilen(&self) -> isize {
        self.data.len().try_into().expect("must fit")
    }

    fn log_length_diff(&self, diff: isize) {
        let Some(logging) = &self.logging else { return };
        if diff > 0 {
            let count = usize::try_from(diff).expect("must fit");
            logging.chain_created(count);
            logging.chain_dropped(0);
        } else if diff < 0 {
            let count = usize::try_from(-diff).expect("must fit");
            logging.chain_created(0);
            logging.chain_dropped(count);
        }
    }
}

impl<D> Drop for Stage<D> {
    fn drop(&mut self) {
        if let Some(logging) = &self.logging {
            logging.chain_dropped(self.data.len());
        }
    }
}

/// Sort and consolidate the given list of updates.
///
/// This function is the same as [`differential_dataflow::consolidation::consolidate_updates`],
/// except that it sorts updates by (time, data) instead of (data, time).
fn consolidate<D: Data>(updates: &mut Vec<(D, Timestamp, Diff)>) {
    if updates.len() <= 1 {
        return;
    }

    let diff = |update: &(_, _, Diff)| update.2;

    updates.sort_unstable_by(|(d1, t1, _), (d2, t2, _)| (t1, d1).cmp(&(t2, d2)));

    let mut offset = 0;
    let mut accum = diff(&updates[0]);

    for idx in 1..updates.len() {
        let this = &updates[idx];
        let prev = &updates[idx - 1];
        if this.0 == prev.0 && this.1 == prev.1 {
            accum += diff(&updates[idx]);
        } else {
            if accum != Diff::ZERO {
                updates.swap(offset, idx - 1);
                updates[offset].2 = accum;
                offset += 1;
            }
            accum = diff(&updates[idx]);
        }
    }

    if accum != Diff::ZERO {
        let len = updates.len();
        updates.swap(offset, len - 1);
        updates[offset].2 = accum;
        offset += 1;
    }

    updates.truncate(offset);
}

/// Compare two columnar refs that have unrelated input lifetimes.
///
/// `<D::Container as Borrow>::Ref<'a>` is an associated-type projection through a trait, so
/// the compiler treats it as invariant in `'a` and won't auto-shorten the inputs by variance.
/// We instead explicitly reborrow both to a fresh, local lifetime `'x` via
/// [`Columnar::reborrow`] before letting the inner `==` pick up the `for<'a> Ref<'a>: Eq`
/// bound on [`Data`].
#[inline]
fn refs_eq<D: Data>(a: Ref<'_, D>, b: Ref<'_, D>) -> bool {
    #[inline]
    fn eq<'x, D: Data>(a: Ref<'x, D>, b: Ref<'x, D>) -> bool {
        a == b
    }
    eq::<D>(D::reborrow(a), D::reborrow(b))
}

/// A binary heap specialized for merging [`Cursor`]s.
struct MergeHeap<D: Data>(BinaryHeap<MergeCursor<D>>);

impl<D: Data> FromIterator<Cursor<D>> for MergeHeap<D> {
    fn from_iter<I: IntoIterator<Item = Cursor<D>>>(cursors: I) -> Self {
        let inner = cursors.into_iter().map(MergeCursor).collect();
        Self(inner)
    }
}

impl<D: Data> MergeHeap<D> {
    /// Pop the next cursor (the one yielding the least update) from the heap.
    fn pop(&mut self) -> Option<Cursor<D>> {
        self.0.pop().map(|MergeCursor(c)| c)
    }

    /// Pop the next cursor from the heap, provided the data and time of its current update are
    /// equal to the given values.
    ///
    /// Returns both the cursor and the diff corresponding to `data` and `time`.
    fn pop_equal(&mut self, data: Ref<'_, D>, time: Timestamp) -> Option<(Cursor<D>, Diff)> {
        let r = {
            let MergeCursor(cursor) = self.0.peek()?;
            let (d, t, r) = cursor.get();
            if t != time || !refs_eq::<D>(d, data) {
                return None;
            }
            r
        };
        let cursor = self.pop().expect("checked above");
        Some((cursor, r))
    }

    /// Push a cursor onto the heap.
    fn push(&mut self, cursor: Cursor<D>) {
        self.0.push(MergeCursor(cursor));
    }
}

/// A wrapper for [`Cursor`]s on a [`MergeHeap`].
///
/// Implements the cursor ordering required for merging cursors.
struct MergeCursor<D: Data>(Cursor<D>);

impl<D: Data> PartialEq for MergeCursor<D> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl<D: Data> Eq for MergeCursor<D> {}

impl<D: Data> PartialOrd for MergeCursor<D> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<D: Data> Ord for MergeCursor<D> {
    fn cmp(&self, other: &Self) -> Ordering {
        let (d1, t1, _) = self.0.get();
        let (d2, t2, _) = other.0.get();
        (t1, d1).cmp(&(t2, d2)).reverse()
    }
}

#[cfg(test)]
mod tests {
    use mz_ore::metrics::MetricsRegistry;
    use mz_persist_client::cfg::PersistConfig;
    use mz_persist_client::metrics::Metrics;
    use mz_repr::{Diff, Timestamp};

    use super::*;
    use crate::sink::correction::CorrectionV1;

    #[mz_ore::test]
    fn chain_builder_update_count_matches_items() {
        let mut builder = ChainBuilder::<i64>::default();
        for i in 0..10_u64 {
            let d = i64::try_from(i).expect("fits");
            builder.push_owned(&(d, Timestamp::new(i), Diff::ONE));
        }
        let chain = builder.finish();
        assert_eq!(chain.update_count, chain.iter().count());
    }

    /// Push enough updates to cross at least one `mint()` boundary, forcing the
    /// `Align` encode -> `from_bytes` decode roundtrip (the spilling path this data
    /// structure exists to support), and assert `iter()` roundtrips values, order,
    /// and diffs across the spill boundary.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow: crossing the ~2 MiB mint boundary needs ~200k updates
    fn chain_builder_roundtrips_across_mint_boundary() {
        // A single `mint()` fires near the ~2 MiB (`SHIP_WORDS`) serialized boundary. With
        // three 8-byte columns per update that's tens of thousands of updates; pushing 200k
        // comfortably forces multiple mints.
        let count = 200_000_u64;

        let mut builder = ChainBuilder::<i64>::default();
        for i in 0..count {
            let d = i64::try_from(i).expect("fits");
            builder.push_owned(&(d, Timestamp::new(i), Diff::ONE));
        }
        let chain = builder.finish();

        // Crossing the mint boundary must have produced more than one chunk; otherwise the
        // multi-chunk read path wouldn't be exercised. The chunk payload may live in the buffer
        // pool (see [`Chunk`]), so we assert on chunk count rather than inspecting the column
        // variant directly.
        assert!(
            chain.chunks.len() > 1,
            "expected multiple minted chunks, got {} chunk(s): {:?}",
            chain.chunks.len(),
            chain.chunks,
        );

        // `iter()` must roundtrip every update, in order, with correct diffs.
        assert_eq!(chain.update_count, usize::try_from(count).expect("fits"));
        let mut expected = 0_u64;
        for (d, t, r) in chain.iter() {
            assert_eq!(d, i64::try_from(expected).expect("fits"));
            assert_eq!(t, Timestamp::new(expected));
            assert_eq!(r, Diff::ONE);
            expected += 1;
        }
        assert_eq!(expected, count);
    }

    fn sink_metrics() -> SinkMetrics {
        let registry = MetricsRegistry::new();
        let metrics = Metrics::new(&PersistConfig::new_for_tests(), &registry);
        metrics.sink.clone()
    }

    /// Run the same stepwise-drain workload through `CorrectionV1` and `CorrectionV2` and assert
    /// that they emit the same updates at every step.
    ///
    /// Models the `write_batches` operator catching up through many distinct timestamps: the
    /// desired input runs ahead, batches are written one timestamp at a time, and written updates
    /// come back negated through the persist feedback.
    #[mz_ore::test]
    // Columnation regions are not Stacked Borrows compliant: later pushes invalidate the
    // provenance of previously stored items under Miri.
    #[cfg_attr(miri, ignore)]
    fn equivalence_with_v1() {
        let sink_metrics = sink_metrics();

        let mut v1 =
            CorrectionV1::<String>::new(sink_metrics.clone(), sink_metrics.for_worker(0), 1);
        let mut v2 = CorrectionV2::<String>::new(
            sink_metrics.clone(),
            sink_metrics.for_worker(0),
            None,
            3.0,
            8 * 1024,
        );

        let num_ts = 50;
        let keys = 4;

        // Upsert-style input: every timestamp updates each key, retracting the previous value.
        let batch = |t: u64| -> Vec<(String, Timestamp, Diff)> {
            (0..keys)
                .flat_map(|k| {
                    let addition = (format!("{k}-{t}"), Timestamp::from(t), Diff::ONE);
                    let retraction = t
                        .checked_sub(1)
                        .map(|p| (format!("{k}-{p}"), Timestamp::from(t), -Diff::ONE));
                    std::iter::once(addition).chain(retraction)
                })
                .collect()
        };

        // Pre-fill both with all batches, like a catch-up where the input runs ahead.
        for t in 0..num_ts {
            v1.insert(&mut batch(t));
            v2.insert(&mut batch(t));
        }

        // Drain stepwise, with persist feedback, comparing emissions.
        for t in 0..num_ts {
            let upper = Antichain::from_elem(Timestamp::from(t + 1));

            let mut out1: Vec<_> = v1.updates_before(&upper).collect();
            let mut out2: Vec<_> = v2.updates_before(&upper).collect();
            out1.sort();
            out2.sort();
            assert_eq!(out1, out2, "diverged at t={t}");

            v1.insert_negated(&mut out1.clone());
            v2.insert_negated(&mut out2);
            v1.advance_since(upper.clone());
            v2.advance_since(upper);
        }

        // Compare the final state at the since.
        let upper = Antichain::from_elem(Timestamp::from(num_ts + 1));
        v1.consolidate_at_since();
        v2.consolidate_at_since();
        let mut out1: Vec<_> = v1.updates_before(&upper).collect();
        let mut out2: Vec<_> = v2.updates_before(&upper).collect();
        out1.sort();
        out2.sort();
        assert_eq!(out1, out2);
    }

    /// A since jump across many distinct buffered timestamps must collapse them onto the since.
    #[mz_ore::test]
    // Columnation regions are not Stacked Borrows compliant: later pushes invalidate the
    // provenance of previously stored items under Miri.
    #[cfg_attr(miri, ignore)]
    fn since_jump() {
        let sink_metrics = sink_metrics();
        let mut v2 = CorrectionV2::<String>::new(
            sink_metrics.clone(),
            sink_metrics.for_worker(0),
            None,
            3.0,
            8 * 1024,
        );

        let num_ts = 100;
        for t in 0..num_ts {
            v2.insert(&mut vec![
                (format!("a-{t}"), Timestamp::from(t), Diff::ONE),
                (format!("a-{t}"), Timestamp::from(t), -Diff::ONE),
                (format!("b-{t}"), Timestamp::from(t), Diff::ONE),
            ]);
        }

        v2.advance_since(Antichain::from_elem(Timestamp::from(num_ts)));
        v2.consolidate_at_since();

        let upper = Antichain::from_elem(Timestamp::from(num_ts + 1));
        let out: Vec<_> = v2.updates_before(&upper).collect();
        assert_eq!(out.len(), usize::try_from(num_ts).unwrap());
        assert!(
            out.iter()
                .all(|(_, t, r)| *t == Timestamp::from(num_ts) && *r == Diff::ONE)
        );
    }

    /// Reads must not observe updates at or beyond their `upper`, even when the `upper` is not
    /// beyond the `since`.
    #[mz_ore::test]
    // Columnation regions are not Stacked Borrows compliant: later pushes invalidate the
    // provenance of previously stored items under Miri.
    #[cfg_attr(miri, ignore)]
    fn upper_not_beyond_since() {
        let sink_metrics = sink_metrics();
        let mut v2 = CorrectionV2::<String>::new(
            sink_metrics.clone(),
            sink_metrics.for_worker(0),
            None,
            3.0,
            8 * 1024,
        );

        v2.insert(&mut vec![(
            "a".to_owned(),
            Timestamp::from(5_u64),
            Diff::ONE,
        )]);
        v2.advance_since(Antichain::from_elem(Timestamp::from(10_u64)));

        // The update logically lives at time 10 now, so a read before 7 must be empty.
        let upper = Antichain::from_elem(Timestamp::from(7_u64));
        assert_eq!(v2.updates_before(&upper).count(), 0);

        // A read before 11 must emit it, advanced to the since.
        let upper = Antichain::from_elem(Timestamp::from(11_u64));
        let out: Vec<_> = v2.updates_before(&upper).collect();
        assert_eq!(
            out,
            vec![("a".to_owned(), Timestamp::from(10_u64), Diff::ONE)]
        );
    }

    /// Make the process buffer pool the active spill mechanism for the duration of `f`, with a
    /// zero resident budget so every spilled body is evicted and reads drive the actual
    /// read-back path in [`Chunk::column`], then restore an unlimited budget. The pool
    /// configuration is process-wide; concurrent tests only ever observe a correct round-trip
    /// regardless of configuration, so racing on it is benign.
    fn with_spill_pool<R>(f: impl FnOnce() -> R) -> R {
        let spill_all = pool_config::PoolPagerConfig {
            budget_bytes: 0,
            spill_threads: 0,
            eager_backing: false,
            rss_target_bytes: 0,
        };
        assert!(
            pool_config::apply_pool_config(spill_all),
            "buffer pool unavailable",
        );
        let result = f();
        pool_config::apply_pool_config(pool_config::PoolPagerConfig {
            budget_bytes: usize::MAX,
            ..spill_all
        });
        result
    }

    /// Build a chain crossing the mint boundary while every chunk is spilled to the buffer pool,
    /// then assert `iter()` (the read path behind `updates_before`) reads each chunk back and
    /// roundtrips values, order, and diffs.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // the pool's mmap-backed regions are unsupported under miri
    fn iter_roundtrips_through_pool() {
        let count = 200_000_u64;
        with_spill_pool(|| {
            let mut builder = ChainBuilder::<i64>::default();
            for i in 0..count {
                let d = i64::try_from(i).expect("fits");
                builder.push_owned(&(d, Timestamp::new(i), Diff::ONE));
            }
            let chain = builder.finish();
            assert!(chain.chunks.len() > 1, "expected multiple minted chunks");
            assert_eq!(chain.update_count, usize::try_from(count).expect("fits"));

            let mut expected = 0_u64;
            for (d, t, r) in chain.iter() {
                assert_eq!(d, i64::try_from(expected).expect("fits"));
                assert_eq!(t, Timestamp::new(expected));
                assert_eq!(r, Diff::ONE);
                expected += 1;
            }
            assert_eq!(expected, count);
        });
    }

    /// Drive a [`Cursor`] over a spilled, multi-chunk chain to completion (the access pattern
    /// merges use). Each step reads the front chunk back from the pool via [`Chunk::column`];
    /// assert the cursor yields every update in order.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // the pool's mmap-backed regions are unsupported under miri
    fn cursor_steps_through_pool() {
        let count = 200_000_u64;
        with_spill_pool(|| {
            let mut builder = ChainBuilder::<i64>::default();
            for i in 0..count {
                let d = i64::try_from(i).expect("fits");
                builder.push_owned(&(d, Timestamp::new(i), Diff::ONE));
            }
            let chain = builder.finish();
            assert!(chain.chunks.len() > 1, "expected multiple minted chunks");

            let mut rest = chain.into_cursor();
            let mut expected = 0_u64;
            while let Some(cursor) = rest.take() {
                let (d, t, r) = cursor.get();
                assert_eq!(i64::into_owned(d), i64::try_from(expected).expect("fits"));
                assert_eq!(t, Timestamp::new(expected));
                assert_eq!(r, Diff::ONE);
                expected += 1;
                rest = cursor.step();
            }
            assert_eq!(expected, count);
        });
    }
}
