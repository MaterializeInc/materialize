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
use std::sync::atomic::{self, AtomicUsize};
use std::sync::{Arc, Mutex, OnceLock};

use columnar::{Columnar, Index, Len, Ref};
use mz_ore::cast::CastLossy;
use mz_ore::soft_assert_or_log;
use mz_persist_client::metrics::{SinkMetrics, SinkWorkerMetrics, UpdateDelta};
use mz_repr::{Diff, Timestamp};
use mz_timely_util::column_pager::{self, PagedColumn};
use mz_timely_util::columnar::Column;
use mz_timely_util::temporal::{Bucket, BucketChain};
use timely::PartialOrder;
use timely::dataflow::channels::ContainerBytes;
use timely::progress::Antichain;

use crate::sink::correction::{ChannelLogging, SizeMetrics};

/// Convenient alias for use in data trait bounds.
///
/// `D` is constrained to be `Columnar`, so that updates can be stored in a single columnar
/// region per chunk, and the variable-length payload (e.g. `Row` bytes) lives in the same
/// allocation as the rest of the chunk. [`DataContainer`] carries the bounds on that container.
pub trait Data:
    differential_dataflow::Data + Columnar<Container: DataContainer> + Send + Sync
{
}
impl<D> Data for D where
    D: differential_dataflow::Data + Columnar<Container: DataContainer> + Send + Sync
{
}

/// The bounds [`Data`] places on its columnar container.
///
/// The `Ref`-level `Eq + Ord` bounds let the merge/heap code compare updates directly through
/// the columnar borrow, avoiding `into_owned` clones on the hot path. The `Borrowed`-level
/// `Send` bound lets a hoisted `Chunk::view` travel with the iterators that
/// [`CorrectionV2::updates_before`] hands across the persist writer's `await`.
pub trait DataContainer:
    Send + Sync + Clone + for<'a> columnar::Borrow<Ref<'a>: Eq + Ord, Borrowed<'a>: Send>
{
}
impl<C> DataContainer for C where
    C: Send + Sync + Clone + for<'a> columnar::Borrow<Ref<'a>: Eq + Ord, Borrowed<'a>: Send>
{
}

/// A borrowed view over a [`Chunk`]'s column.
///
/// Obtained from [`Chunk::view`] and indexed with `get`.
type ChunkView<'a, D> =
    <<(D, Timestamp, Diff) as Columnar>::Container as columnar::Borrow>::Borrowed<'a>;

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

    /// Total count of updates last reported to metrics.
    ///
    /// Tracked to compute deltas in `update_metrics`.
    prev_update_count: usize,
    /// Total size last reported to metrics.
    ///
    /// Tracked to compute deltas in `update_metrics`.
    prev_size: SizeMetrics,
    /// Global persist sink metrics.
    metrics: SinkMetrics,
    /// Per-worker persist sink metrics.
    worker_metrics: SinkWorkerMetrics,
    /// Running totals and introspection logging.
    accounting: Accounting,
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

        let accounting = Accounting::new(logging);

        Self {
            chain: BucketChain::new(ChainBucket::new(chain_proportionality, accounting.clone())),
            pending_low: Vec::new(),
            emitted: Chain::new(),
            stage: Stage::new(accounting.clone(), chunk_capacity),
            boundary: Antichain::from_elem(Timestamp::MIN),
            since: Antichain::from_elem(Timestamp::MIN),
            prev_update_count: 0,
            prev_size: Default::default(),
            metrics,
            worker_metrics,
            accounting,
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
                self.account_chain_created(&chain);
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
        self.consolidate_before(upper);
        self.consolidated_updates_before(upper)
    }

    /// Return the updates before the given `upper`, as consolidated by a preceding
    /// [`CorrectionV2::consolidate_before`] call.
    ///
    /// The caller must have invoked `consolidate_before` with the same `upper` and must not have
    /// mutated the buffer since. Otherwise the returned updates are neither consolidated nor
    /// necessarily complete.
    pub fn consolidated_updates_before<'a>(
        &'a self,
        upper: &Antichain<Timestamp>,
    ) -> impl Iterator<Item = (D, Timestamp, Diff)> + Send + use<'a, D> {
        // All contained times are advanced to at least the `since`, so a read at an `upper` that
        // is not beyond the `since` is always empty. This mirrors the short-circuit in
        // `consolidate_before`, which leaves `emitted` untouched in that case.
        if !PartialOrder::less_than(&self.since, upper) {
            return None.into_iter().flatten();
        }

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
    /// consolidated.
    ///
    /// Does nothing if `upper` is not beyond the `since`: all contained times are advanced to at
    /// least the `since`, so such a read is empty anyway, and skipping avoids an eager peel,
    /// merge, and `boundary` advancement. Normal reads and `consolidate_at_since` always pass an
    /// `upper` beyond the `since`.
    pub fn consolidate_before(&mut self, upper: &Antichain<Timestamp>) {
        if !PartialOrder::less_than(&self.since, upper) {
            return;
        }

        if let Some(mut ready) = self.stage.flush() {
            self.route(&mut ready);
        }

        let Some(&since_ts) = self.since.as_option() else {
            // If the since is the empty frontier, discard all updates.
            let peeled = self.chain.peel(Antichain::new().borrow());
            for bucket in peeled {
                for chain in bucket.into_chains() {
                    self.account_chain_dropped(&chain);
                }
            }
            for chain in std::mem::take(&mut self.pending_low) {
                self.account_chain_dropped(&chain);
            }
            let emitted = std::mem::replace(&mut self.emitted, Chain::new());
            if !emitted.is_empty() {
                self.account_chain_dropped(&emitted);
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

        candidates
            .iter()
            .for_each(|c| self.account_chain_dropped(c));

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
                        self.account_chain_created(&remainder);
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

        let merged = if stale_times == 0 {
            let cursors: Vec<_> = lowers.into_iter().filter_map(Chain::into_cursor).collect();
            merge_cursors(cursors)
        } else if stale_times < MAX_STALE_RUNS {
            let mut runs = Vec::new();
            for chain in lowers {
                if let Some(cursor) = chain.into_cursor() {
                    runs.append(&mut cursor.advance_by(since_ts));
                }
            }
            merge_cursors(runs)
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
                        self.account_chain_created(&remainder);
                        self.pending_low.push(remainder);
                    }
                    lower
                }
                None => chain,
            }
        };

        if !merged.is_empty() {
            self.account_chain_created(&merged);
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

    fn account_chain_created(&self, chain: &Chain<D>) {
        self.accounting.chain_created(chain);
    }

    fn account_chain_dropped(&self, chain: &Chain<D>) {
        self.accounting.chain_dropped(chain);
    }

    /// Update persist sink metrics.
    ///
    /// Reads the running totals maintained by [`Accounting`], so its cost is independent of how
    /// much the buffer holds. Nothing here walks chains or pages a chunk in.
    fn update_metrics(&mut self) {
        let (new_length, new_size) = self.accounting.totals();
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

        self.accounting.report_size_metrics(new_size, old_size);

        self.prev_size = new_size;
        self.prev_update_count = new_length;
    }
}

/// Merge the given cursors into one chain.
fn merge_cursors<D: Data>(cursors: Vec<Cursor<D>>) -> Chain<D> {
    match cursors.len() {
        0 => Chain::new(),
        1 => {
            let [cur] = cursors.try_into().unwrap();
            cur.into_chain()
        }
        2 => {
            let [a, b] = cursors.try_into().unwrap();
            merge_2(a, b)
        }
        _ => merge_many(cursors),
    }
}

/// Merge the given two cursors using a 2-way merge.
///
/// This function is a specialization of `merge_many` that avoids the overhead of a binary heap.
fn merge_2<D: Data>(cursor1: Cursor<D>, cursor2: Cursor<D>) -> Chain<D> {
    let mut rest1 = Some(cursor1);
    let mut rest2 = Some(cursor2);
    let mut merged = ChainBuilder::default();

    // One borrow per chunk pair, not per update: `Chunk::view` re-decodes the column header on
    // every call. The inner loop runs until either cursor crosses into its next chunk, at which
    // point the outer loop re-borrows both.
    while rest1.is_some() && rest2.is_some() {
        let chunk1 = rest1.as_ref().expect("checked above").chunk_handle();
        let chunk2 = rest2.as_ref().expect("checked above").chunk_handle();
        let view1 = chunk1.view();
        let view2 = chunk2.view();

        loop {
            let (Some(c1), Some(c2)) = (rest1.as_ref(), rest2.as_ref()) else {
                break;
            };
            if !c1.reads_from(&chunk1) || !c2.reads_from(&chunk2) {
                break;
            }

            let (d1, t1, r1) = c1.get_with(&view1);
            let (d2, t2, r2) = c2.get_with(&view2);

            match refs_cmp::<D>((t1, d1), (t2, d2)) {
                Ordering::Less => {
                    merged.push_ref((d1, t1, r1));
                    rest1 = rest1.take().expect("checked above").step();
                }
                Ordering::Greater => {
                    merged.push_ref((d2, t2, r2));
                    rest2 = rest2.take().expect("checked above").step();
                }
                Ordering::Equal => {
                    let r = r1 + r2;
                    if r != Diff::ZERO {
                        merged.push_ref((d1, t1, r));
                    }
                    rest1 = rest1.take().expect("checked above").step();
                    rest2 = rest2.take().expect("checked above").step();
                }
            }
        }
    }

    match (rest1, rest2) {
        (Some(c), None) | (None, Some(c)) => merged.push_cursor(c),
        (Some(_), Some(_)) => unreachable!("loop runs while both cursors are live"),
        (None, None) => (),
    }

    merged.finish()
}

/// Merge the given cursors using a k-way merge with a binary heap.
fn merge_many<D: Data>(cursors: Vec<Cursor<D>>) -> Chain<D> {
    let mut heap = MergeHeap::from_iter(cursors);
    let mut merged = ChainBuilder::default();
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
            bucket
                .chains
                .iter()
                .for_each(|c| self.account_chain_dropped(c));
        }
        self.pending_low
            .iter()
            .for_each(|c| self.account_chain_dropped(c));
        if !self.emitted.is_empty() {
            self.account_chain_dropped(&self.emitted);
        }
        self.update_metrics_inner(Default::default(), 0);
    }
}

/// Running totals over the buffer's contents, plus the introspection logging that reports them.
///
/// Shared by the buffer and by every structure that holds its chains, so the totals are
/// maintained where chains are minted and retired rather than by a walk over the buffer.
///
/// Correctness rests on a discipline the code already keeps for the introspection logging: a
/// chain the buffer holds has been announced exactly once through [`Accounting::chain_created`],
/// and is retired exactly once, through [`Accounting::chain_dropped`], before it is dropped or
/// consumed. Chains are immutable once announced, so the totals a chain contributes at
/// retirement are the ones it contributed at announcement. `metrics_totals_match_walk` checks
/// the result against a walk.
#[derive(Clone, Debug)]
struct Accounting {
    /// The totals, shared with every clone.
    totals: Arc<Totals>,
    /// Introspection logging, absent when the sink is not logging.
    logging: Option<ChannelLogging>,
}

/// The running totals behind an [`Accounting`].
///
/// Atomics because the buffer must stay `Send`, not because the counters are contended: the sink
/// touches its buffer from one thread at a time, which is why `Relaxed` suffices.
#[derive(Debug, Default)]
struct Totals {
    /// Number of updates the buffer holds.
    records: AtomicUsize,
    /// Serialized size of what the buffer holds, in bytes.
    size: AtomicUsize,
    /// Number of allocations holding it: one per chunk, plus the staging vector while it has one.
    allocations: AtomicUsize,
}

impl Accounting {
    /// Construct accounting that reports to the given logging, if any.
    fn new(logging: Option<ChannelLogging>) -> Self {
        Self {
            totals: Default::default(),
            logging,
        }
    }

    /// Return the current totals as `(records, size metrics)`.
    ///
    /// The reported capacity equals the size: chunk bodies are exactly sized when minted, and the
    /// staging vector's slack is bounded by one chunk's worth of updates and not tracked.
    fn totals(&self) -> (usize, SizeMetrics) {
        let size = self.totals.size.load(atomic::Ordering::Relaxed);
        let metrics = SizeMetrics {
            size,
            capacity: size,
            allocations: self.totals.allocations.load(atomic::Ordering::Relaxed),
        };
        (self.totals.records.load(atomic::Ordering::Relaxed), metrics)
    }

    /// Account for a chain the buffer now holds.
    fn chain_created<D: Data>(&self, chain: &Chain<D>) {
        let relaxed = atomic::Ordering::Relaxed;
        self.totals.records.fetch_add(chain.update_count, relaxed);
        self.totals.size.fetch_add(chain.size, relaxed);
        self.totals
            .allocations
            .fetch_add(chain.chunks.len(), relaxed);
        if let Some(logging) = &self.logging {
            logging.chain_created(chain.update_count);
        }
    }

    /// Account for a chain the buffer no longer holds.
    fn chain_dropped<D: Data>(&self, chain: &Chain<D>) {
        let relaxed = atomic::Ordering::Relaxed;
        self.totals.records.fetch_sub(chain.update_count, relaxed);
        self.totals.size.fetch_sub(chain.size, relaxed);
        self.totals
            .allocations
            .fetch_sub(chain.chunks.len(), relaxed);
        if let Some(logging) = &self.logging {
            logging.chain_dropped(chain.update_count);
        }
    }

    /// Announce the staging area as an empty chain, which is how its population is reported.
    fn stage_created(&self) {
        if let Some(logging) = &self.logging {
            logging.chain_created(0);
        }
    }

    /// Account for the staging area changing by `records` updates of `update_size` bytes each,
    /// and by `allocations` allocations.
    fn stage_diff(&self, records: isize, allocations: isize, update_size: usize) {
        let bytes = records * isize::try_from(update_size).expect("must fit");
        add_signed(&self.totals.records, records);
        add_signed(&self.totals.size, bytes);
        add_signed(&self.totals.allocations, allocations);

        // The stage is reported as a chain that is dropped and re-created at its new length.
        let Some(logging) = &self.logging else { return };
        if records > 0 {
            logging.chain_created(usize::try_from(records).expect("positive"));
            logging.chain_dropped(0);
        } else if records < 0 {
            logging.chain_created(0);
            logging.chain_dropped(usize::try_from(-records).expect("positive"));
        }
    }

    /// Report the change from `old` to `new` size metrics to introspection.
    fn report_size_metrics(&self, new: SizeMetrics, old: SizeMetrics) {
        let Some(logging) = &self.logging else { return };
        let i = |x: usize| isize::try_from(x).expect("must fit");
        logging.report_size_diff(i(new.size) - i(old.size));
        logging.report_capacity_diff(i(new.capacity) - i(old.capacity));
        logging.report_allocations_diff(i(new.allocations) - i(old.allocations));
    }
}

/// Add a signed `diff` to `counter`.
///
/// # Panics
///
/// Panics if the result would be negative, which means a retirement was never matched by an
/// announcement.
fn add_signed(counter: &AtomicUsize, diff: isize) {
    let relaxed = atomic::Ordering::Relaxed;
    if diff >= 0 {
        counter.fetch_add(usize::try_from(diff).expect("non-negative"), relaxed);
    } else {
        let sub = usize::try_from(-diff).expect("positive");
        let prev = counter.fetch_sub(sub, relaxed);
        assert!(prev >= sub, "total retired below zero");
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
    /// Running totals and introspection logging.
    accounting: Accounting,
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
    fn new(chain_proportionality: f64, accounting: Accounting) -> Self {
        Self {
            chains: Vec::new(),
            chain_proportionality,
            accounting,
        }
    }

    /// Push a chain onto the bucket, restoring the chain invariant.
    fn push_chain(&mut self, chain: Chain<D>) {
        if chain.is_empty() {
            return;
        }
        self.accounting.chain_created(&chain);
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
            self.accounting.chain_dropped(&a);
            self.accounting.chain_dropped(&b);

            let cursors = [a, b].into_iter().filter_map(Chain::into_cursor).collect();
            let merged = merge_cursors(cursors);
            if !merged.is_empty() {
                self.accounting.chain_created(&merged);
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
        let mut lower = Self::new(self.chain_proportionality, self.accounting.clone());
        let mut upper = Self::new(self.chain_proportionality, self.accounting.clone());

        for chain in self.chains {
            // Whole chunks are reused; at most one chunk straddling the timestamp is copied per
            // chain. Account fuel at chunk granularity.
            *fuel = fuel.saturating_sub(i64::try_from(chain.chunks.len()).expect("must fit"));

            self.accounting.chain_dropped(&chain);
            let (lo, hi) = chain.split_at_time(*timestamp);
            for (part, target) in [(lo, &mut lower), (hi, &mut upper)] {
                if !part.is_empty() {
                    target.accounting.chain_created(&part);
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
    /// The serialized size of all chunks, in bytes.
    ///
    /// Maintained as chunks are pushed, so metrics never walk the chunks.
    size: usize,
}

impl<D: Data> Chain<D> {
    /// Construct an empty chain.
    fn new() -> Self {
        Self {
            chunks: Default::default(),
            update_count: 0,
            size: 0,
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
        mz_ore::soft_assert_no_log!(self.can_accept_chunk(&chunk));

        self.update_count += chunk.len();
        self.size += chunk.size();
        self.chunks.push(chunk);
    }

    /// Return whether the chain can accept the given chunk at its end while preserving
    /// (time, data)-order.
    ///
    /// NOTE: The cached boundary times settle every case but a tie. On a tie the boundary updates
    /// themselves are compared, which materializes both chunks and keeps them resident for the
    /// rest of their lifetime. The only caller is the soft assertion in [`Chain::push_chunk`], and
    /// soft assertions are live in any build started with `MZ_SOFT_ASSERTIONS` set, so this cost is
    /// not confined to debug builds. Ties are reached whenever a run of updates at a single
    /// timestamp spans a chunk boundary, which [`ChunkBuilder`] produces for any such run larger
    /// than its byte limit.
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
            let view = c.view();
            (0..c.len()).map(move |i| {
                let (d, t, r) = view.get(i);
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
        let mut lower = Self::new();
        let mut upper = Self::new();

        let Some(skip_ts) = time.step_back() else {
            // Nothing sorts before `time`.
            return (lower, self);
        };

        for chunk in self.chunks.drain(..) {
            // Route whole chunks by cached boundary times, so a chunk that lands entirely on one
            // side is moved without paging it in. Only a straddling chunk is materialized here.
            // With soft assertions on, `push_chunk` can still page in a chunk whose boundary time
            // ties the chain's last one, see `Chain::can_accept_chunk`.
            if chunk.last_time() < time {
                lower.push_chunk(chunk);
            } else if chunk.first_time() >= time {
                upper.push_chunk(chunk);
            } else {
                // The chunk straddles `time`; copy its two halves.
                let idx = chunk
                    .find_time_greater_than(skip_ts)
                    .expect("straddles time");
                let view = chunk.view();
                let mut builder = ChainBuilder::default();
                for i in 0..idx {
                    builder.push_ref(view.get(i));
                }
                for part in builder.finish().chunks {
                    lower.push_chunk(part);
                }
                let mut builder = ChainBuilder::default();
                for i in idx..chunk.len() {
                    builder.push_ref(view.get(i));
                }
                for part in builder.finish().chunks {
                    upper.push_chunk(part);
                }
            }
        }

        (lower, upper)
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
        // One borrow per chunk: see `Chunk::view` for why this must not move into the inner loop.
        while let Some(cursor) = rest.take() {
            let chunk = cursor.chunk_handle();
            let view = chunk.view();
            rest = Some(cursor);

            while let Some(cursor) = rest.as_ref() {
                if !cursor.reads_from(&chunk) {
                    break;
                }
                self.push_ref(cursor.get_with(&view));
                rest = rest.take().expect("checked above").step();
            }
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
    ///
    /// Single-access only. A loop over a cursor must hoist [`Cursor::chunk_handle`]'s
    /// [`Chunk::view`] and read through [`Cursor::get_with`] instead.
    fn get(&self) -> Ref<'_, (D, Timestamp, Diff)> {
        let chunk = self.get_chunk();
        let (d, t, r) = chunk.index(self.chunk_offset);
        let t = self.overwrite_ts.unwrap_or(t);
        (d, t, r)
    }

    /// Get a reference to the current update, reading through an already-borrowed view.
    ///
    /// # Panics
    ///
    /// Panics if `view` is not a view of the cursor's current chunk. Guard loops with
    /// [`Cursor::reads_from`], which is how a caller learns the cursor has crossed into the next
    /// chunk and the view must be refreshed.
    fn get_with<'a>(&self, view: &ChunkView<'a, D>) -> Ref<'a, (D, Timestamp, Diff)> {
        debug_assert_eq!(view.len(), self.get_chunk().len(), "view of another chunk");
        let (d, t, r) = view.get(self.chunk_offset);
        let t = self.overwrite_ts.unwrap_or(t);
        (d, t, r)
    }

    /// A shared handle on the chunk the cursor currently reads from.
    ///
    /// Held by callers that hoist a [`Chunk::view`], so the view's borrow outlives the cursor
    /// steps taken against it.
    fn chunk_handle(&self) -> Rc<Chunk<D>> {
        Rc::clone(&self.chunks[0])
    }

    /// Whether the cursor still reads from `chunk`.
    fn reads_from(&self, chunk: &Rc<Chunk<D>>) -> bool {
        Rc::ptr_eq(&self.chunks[0], chunk)
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

/// A non-empty chunk of updates, backed by a columnar region.
///
/// All updates in a chunk are sorted by (time, data) and consolidated.
///
/// Chunks are immutable once created. They are produced by [`ChunkBuilder`], which mints a
/// new chunk whenever its in-progress columnar container reaches a fixed serialized byte
/// boundary (~2 MiB, matching the ship granularity used elsewhere in the codebase), so each
/// chunk corresponds to a single, predictably sized allocation.
struct Chunk<D: Data> {
    /// The paged-out form, taken on first materialization.
    ///
    /// A `Mutex` (not `RefCell`) keeps the chunk `Sync`: cursors hold chunks behind a shared
    /// `Rc`, and the iterator returned by [`CorrectionV2::updates_before`] borrows them across
    /// the persist writer's `await`, so `&Chunk` must be `Send`. The lock is taken once, at
    /// materialization, and is otherwise uncontended (the sink runs single-threaded per worker).
    paged: Mutex<Option<PagedColumn<(D, Timestamp, Diff)>>>,
    /// The materialized form, populated lazily by [`Chunk::column`] on first access.
    ///
    /// An `OnceLock` (not `OnceCell`) for the same `Sync` reason. Once set the slot is never
    /// cleared, so its address is stable and [`Chunk::index`] can hand out `Ref<'_>` borrows tied
    /// to `&self`. The allocation is freed when the chunk drops, which bounds resident memory to
    /// the chunks under an active merge front.
    resident: OnceLock<Column<(D, Timestamp, Diff)>>,
    /// Number of updates, cached so `len` and chain bookkeeping never page the chunk in.
    len: usize,
    /// Serialized size of the body in bytes, cached so size accounting never pages it in.
    size: usize,
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
    /// Page the given non-empty column out into a chunk.
    ///
    /// Reads the cached metadata (length, boundary times) while the column is still resident, then
    /// hands it to the global column pager. The policy decides whether it actually spills; either
    /// way the chunk is born paged and materializes lazily on first read.
    ///
    /// # Panics
    ///
    /// Panics if the column is empty. Chunks are non-empty by construction; [`ChunkBuilder`] only
    /// ever builds a chunk from a populated column.
    fn from_column(mut data: Column<(D, Timestamp, Diff)>) -> Self {
        let (len, first_time, last_time) = {
            let borrowed = data.borrow();
            let len = borrowed.len();
            assert!(len > 0, "chunks are non-empty");
            (len, borrowed.get(0).1, borrowed.get(len - 1).1)
        };
        let size = data.length_in_bytes();

        let paged = column_pager::global_pager().page(&mut data);
        Self {
            paged: Mutex::new(Some(paged)),
            resident: OnceLock::new(),
            len,
            size,
            first_time,
            last_time,
        }
    }

    /// Materialize the chunk's column, paging it in on first access.
    ///
    /// The returned reference is valid for as long as `&self`: the `OnceLock` slot is never
    /// cleared once populated, so its contents have a stable address.
    fn column(&self) -> &Column<(D, Timestamp, Diff)> {
        self.resident.get_or_init(|| {
            let paged = self
                .paged
                .lock()
                .expect("pager mutex poisoned")
                .take()
                .expect("paged form present until materialized");
            column_pager::global_pager().take(paged)
        })
    }

    /// Return the number of updates in the chunk.
    fn len(&self) -> usize {
        self.len
    }

    /// Borrow the chunk's column, paging it in if necessary.
    ///
    /// Any caller that touches more than one update must hoist this out of its loop and index the
    /// returned view. `Column::borrow` on a serialized column rebuilds the struct-of-arrays view
    /// from the serialized header on every call, so borrowing per element pays that decode per
    /// element.
    fn view(&self) -> ChunkView<'_, D> {
        self.column().borrow()
    }

    /// Return the update at the given index, paging the chunk in if necessary.
    ///
    /// Single-access only. Indexing a hoisted [`Chunk::view`] is the loop form.
    ///
    /// # Panics
    ///
    /// Panics if the given index is not populated.
    fn index(&self, idx: usize) -> Ref<'_, (D, Timestamp, Diff)> {
        self.view().get(idx)
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

        let view = self.view();
        let mut lower = 0;
        let mut upper = self.len;
        while lower < upper {
            let idx = (lower + upper) / 2;
            if view.get(idx).1 > time {
                upper = idx;
            } else {
                lower = idx + 1;
            }
        }

        Some(lower)
    }

    /// Return the serialized size of the chunk's body in bytes, for use in metrics.
    ///
    /// This is the logical size of the body, spilled or not. Whether the body is currently
    /// resident is the pager's business, and it changes without telling the chunk, so a
    /// resident-byte figure would have to page the chunk in or lock the pager to be right.
    fn size(&self) -> usize {
        self.size
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
}

impl<D: Data> Default for ChunkBuilder<D> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
        }
    }
}

impl<D: Data> ChunkBuilder<D> {
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
        self.inner
            .extract()
            .map(|c| Chunk::from_column(std::mem::take(c)))
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
        // empty chunk would needlessly engage the pager.
        std::iter::from_fn(move || {
            loop {
                let col = std::mem::take(self.inner.finish()?);
                if !col.is_empty() {
                    return Some(Chunk::from_column(col));
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
    /// Grows into `chunk_capacity` rather than being allocated at it, so a sink that never
    /// stages that much never holds it. One of these exists per sink and worker, which is why
    /// the eager allocation is worth avoiding even though a staging area is small.
    data: Vec<(D, Timestamp, Diff)>,
    /// How many updates to accumulate before shipping a batch, from
    /// `compute_correction_v2_chunk_size`.
    ///
    /// Shipping less often costs staging memory and saves inserts: it is the number of chains
    /// minted per update, and every chain minted is a chain some later read has to merge.
    chunk_capacity: usize,
    /// Running totals and introspection logging.
    ///
    /// We want to report the number of records in the stage. To do so, we pretend that the stage
    /// is a chain, and every time the number of updates inside changes, the chain gets dropped and
    /// re-created.
    accounting: Accounting,
}

impl<D: Data> Stage<D> {
    fn new(accounting: Accounting, chunk_capacity: usize) -> Self {
        // For logging, we pretend the stage consists of a single chain.
        accounting.stage_created();

        Self {
            data: Vec::new(),
            chunk_capacity,
            accounting,
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

        let before = self.snapshot();

        // Determine how many chunks we can fill with the available updates.
        let update_count = self.data.len() + updates.len();
        let chunk_capacity = self.chunk_capacity;
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

        self.account_since(before);

        maybe_ready
    }

    /// Flush all currently staged updates, returning them sorted and consolidated.
    fn flush(&mut self) -> Option<Vec<(D, Timestamp, Diff)>> {
        let before = self.snapshot();

        consolidate(&mut self.data);
        let data = (!self.data.is_empty()).then(|| std::mem::take(&mut self.data));

        self.account_since(before);
        data
    }

    /// Advance the times of staged updates by the given `since`.
    fn advance_times(&mut self, since: &Antichain<Timestamp>) {
        let Some(since_ts) = since.as_option() else {
            // If the since is the empty frontier, discard all updates.
            let before = self.snapshot();
            self.data.clear();
            self.account_since(before);
            return;
        };

        for (_, time, _) in &mut self.data {
            *time = std::cmp::max(*time, *since_ts);
        }
    }

    /// The staged length and allocation count, taken before a mutation for [`Stage::account_since`].
    fn snapshot(&self) -> (isize, isize) {
        let len = isize::try_from(self.data.len()).expect("must fit");
        (len, isize::from(self.data.capacity() > 0))
    }

    /// Account for the change to the stage since `before`, a [`Stage::snapshot`].
    ///
    /// Sizes count the bare tuples without following pointers, so they are under-estimates.
    /// That is fine as the stage should always be small.
    fn account_since(&self, before: (isize, isize)) {
        let (len, allocations) = self.snapshot();
        self.accounting.stage_diff(
            len - before.0,
            allocations - before.1,
            std::mem::size_of::<(D, Timestamp, Diff)>(),
        );
    }
}

impl<D> Drop for Stage<D> {
    fn drop(&mut self) {
        let len = isize::try_from(self.data.len()).expect("must fit");
        let allocations = isize::from(self.data.capacity() > 0);
        self.accounting.stage_diff(
            -len,
            -allocations,
            std::mem::size_of::<(D, Timestamp, Diff)>(),
        );
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

/// Compare two `(time, data)` pairs of columnar refs that have unrelated input lifetimes.
///
/// The same reborrow as [`refs_eq`], for the `Ord` bound.
#[inline]
fn refs_cmp<D: Data>(a: (Timestamp, Ref<'_, D>), b: (Timestamp, Ref<'_, D>)) -> Ordering {
    #[inline]
    fn cmp<'x, D: Data>(a: (Timestamp, Ref<'x, D>), b: (Timestamp, Ref<'x, D>)) -> Ordering {
        a.cmp(&b)
    }
    cmp::<D>((a.0, D::reborrow(a.1)), (b.0, D::reborrow(b.1)))
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

        // Crossing the mint boundary must have produced more than one chunk; otherwise the spill
        // path (each minted chunk is paged out and read back through the pager) wouldn't be
        // exercised. The chunk payload itself is now behind the pager (see [`Chunk`]), so we
        // assert on chunk count rather than inspecting the column variant directly.
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

    /// The maintained record, size, and allocation totals must equal a walk over the chunks.
    ///
    /// Guards the delta accounting in [`Chain::push_chunk`] and [`Stage::account_since`]: a
    /// site that grows a chain or the stage without adjusting the totals shows up here.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // slow under Miri, and the bookkeeping it checks has no unsafe code
    fn metrics_totals_match_walk() {
        let sink_metrics = sink_metrics();
        let mut v2 = CorrectionV2::<String>::new(
            sink_metrics.clone(),
            sink_metrics.for_worker(0),
            None,
            3.0,
            8 * 1024,
        );

        let num_ts = 200_u64;
        for t in 0..num_ts {
            v2.insert(&mut vec![
                (format!("a-{t}"), Timestamp::from(t), Diff::ONE),
                (format!("b-{t}"), Timestamp::from(t), Diff::ONE),
            ]);
        }

        // Drain part of the buffer, so chains sit in `emitted` and `pending_low` as well as in
        // the bucket chain.
        let upper = Antichain::from_elem(Timestamp::from(num_ts / 2));
        let drained = v2.updates_before(&upper).count();
        assert_eq!(drained, usize::try_from(num_ts).expect("fits"));

        // Advancing the since and consolidating there exercises the peel, split, and merge paths,
        // each of which retires and mints chains.
        v2.advance_since(Antichain::from_elem(Timestamp::from(num_ts / 2)));
        v2.consolidate_at_since();
        v2.insert(&mut vec![(
            "late".to_owned(),
            Timestamp::from(num_ts),
            Diff::ONE,
        )]);

        let mut chains: Vec<&Chain<String>> = vec![&v2.emitted];
        chains.extend(&v2.pending_low);
        for bucket in v2.chain.buckets() {
            chains.extend(&bucket.chains);
        }

        let mut size = v2.stage.data.len() * std::mem::size_of::<(String, Timestamp, Diff)>();
        let mut records = v2.stage.data.len();
        let mut allocations = usize::from(v2.stage.data.capacity() > 0);
        for chain in chains {
            size += chain.chunks.iter().map(Chunk::size).sum::<usize>();
            records += chain.chunks.iter().map(Chunk::len).sum::<usize>();
            allocations += chain.chunks.len();
        }

        assert!(size > 0, "workload must leave chunks behind");
        assert_eq!(v2.prev_size.size, size);
        assert_eq!(v2.prev_size.capacity, size);
        assert_eq!(v2.prev_size.allocations, allocations);
        assert_eq!(v2.prev_update_count, records);
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

    /// A [`PagingPolicy`] that always spills to the swap backend, uncompressed.
    ///
    /// The default global pager keeps every chunk resident; installing this drives the actual
    /// spill path so the tests exercise [`Chunk::column`]'s page-in through [`mz_ore::pager`].
    ///
    /// [`PagingPolicy`]: column_pager::PagingPolicy
    struct ForceSwap;

    impl column_pager::PagingPolicy for ForceSwap {
        fn decide(&self, _hint: column_pager::PageHint) -> column_pager::PageDecision {
            column_pager::PageDecision::Page {
                backend: mz_ore::pager::Backend::Swap,
                codec: None,
            }
        }
        fn record(&self, _event: column_pager::PageEvent) {}
    }

    /// Install a global pager that spills every chunk to swap for the duration of `f`, then
    /// restore the default (disabled) pager. The global pager is process-wide; concurrent tests
    /// only ever observe a correct round-trip regardless of backend, so racing on it is benign.
    fn with_swap_pager<R>(f: impl FnOnce() -> R) -> R {
        use std::sync::Arc;
        column_pager::set_global_pager(column_pager::ColumnPager::new(Arc::new(ForceSwap)));
        let result = f();
        column_pager::set_global_pager(column_pager::ColumnPager::disabled());
        result
    }

    /// Build a chain crossing the mint boundary while every chunk is spilled to swap, then assert
    /// `iter()` (the read path behind `updates_before`) pages each chunk back in and roundtrips
    /// values, order, and diffs.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // madvise on the swap backend is unsupported under miri
    fn iter_roundtrips_through_swap_backend() {
        let count = 200_000_u64;
        with_swap_pager(|| {
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
    /// merges use). Each step pages the front chunk back in via [`Chunk::column`]; assert the
    /// cursor yields every update in order.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // madvise on the swap backend is unsupported under miri
    fn cursor_steps_through_swap_backend() {
        let count = 200_000_u64;
        with_swap_pager(|| {
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
