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
//! [`CorrectionV2`] holds onto a list of [`Chain`]s containing [`Chunk`]s of stashed updates. Each
//! [`Chunk`] is a columnation region containing a fixed maximum number of updates. All updates in
//! a chunk, and all updates in a chain, are ordered by (time, data) and consolidated.
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
//! The "chain invariant" states that each chain has at least [`CHAIN_PROPORTIONALITY`] times as
//! many chunks as the next one. This means that chain sizes will often be powers of
//! `CHAIN_PROPORTIONALITY`, but they don't have to be. For example, for a proportionality of 2,
//! the chain sizes `[11, 5, 2, 1]` would satisfy the chain invariant.
//!
//! Choosing the `CHAIN_PROPORTIONALITY` value allows tuning the trade-off between memory and CPU
//! resources required to maintain corrections. A higher proportionality forces more frequent chain
//! merges, and therefore consolidation, reducing memory usage but increasing CPU usage.
//!
//! ## Inserting Updates
//!
//! A batch of updates is appended as a new chain. Then chains are merged at the end of the chain
//! list until the chain invariant is restored.
//!
//! The insert operation has an amortized complexity of O(log N), with N being the current number
//! of updates stored.
//!
//! ## Retrieving Consolidated Updates
//!
//! Retrieving consolidated updates before a given `upper` works by first consolidating all updates
//! at times before the `upper`, merging them all into one chain, then returning an iterator over
//! that chain.
//!
//! Because each chain contains updates ordered by time first, consolidation of all updates before
//! an `upper` is possible without touching updates at future times. It works by merging the chains
//! only up to the `upper`, producing a merged chain containing consolidated times before the
//! `upper` and leaving behind the chain parts containing later times. The complexity of this
//! operation is O(U log K), with U being the number of updates before `upper` and K the number
//! of chains.
//!
//! Unfortunately, performing consolidation as described above can break the chain invariant and we
//! might need to restore it by merging chains, including ones containing future updates. This is
//! something that would be great to fix! In the meantime the hope is that in steady state it
//! doesn't matter too much because either there are no future retractions and U is approximately
//! equal to N, or the amount of future retractions is much larger than the amount of current
//! changes, in which case removing the current changes has a good chance of leaving the chain
//! invariant intact.
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
//! Instead we need to merge sub-chains, one for each distinct time that's before or at the
//! `since`. Each of these sub-chains retains the (time, data) ordering after the time advancement
//! to `since`, so merging those yields the expected result.
//!
//! For the above example, the chains we would merge are:
//!   chain 1.a: [(c, 2, +1)]
//!   chain 1.b: [(b, 2, -1), (a, 3, -1)]
//!   chain 2.a: [(b, 2, +1)],
//!   chain 2.b: [(a, 2, +1), (c, 2, -1)]

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, VecDeque};
use std::fmt;
use std::rc::Rc;

use differential_dataflow::trace::implementations::BatchContainer;
use mz_persist_client::metrics::{SinkMetrics, SinkWorkerMetrics, UpdateDelta};
use mz_repr::{Diff, Timestamp};
use mz_timely_util::containers::stack::StackWrapper;
use timely::container::columnation::Columnation;
use timely::container::SizableContainer;
use timely::progress::Antichain;
use timely::{Container, PartialOrder};

use crate::sink::correction::LengthAndCapacity;

/// Determines the size factor of subsequent chains required by the chain invariant.
const CHAIN_PROPORTIONALITY: usize = 3;

/// Convenient alias for use in data trait bounds.
pub trait Data: differential_dataflow::Data + Columnation {}
impl<D: differential_dataflow::Data + Columnation> Data for D {}

/// A data structure used to store corrections in the MV sink implementation.
///
/// In contrast to `CorrectionV1`, this implementation stores updates in columnation regions,
/// allowing their memory to be transparently spilled to disk.
#[derive(Debug)]
pub(super) struct CorrectionV2<D: Data> {
    /// Chains containing sorted updates.
    chains: Vec<Chain<D>>,
    /// The frontier by which all contained times are advanced.
    since: Antichain<Timestamp>,

    /// Total length and capacity of chunks in `chains`.
    ///
    /// Tracked to maintain metrics.
    total_size: LengthAndCapacity,
    /// Global persist sink metrics.
    metrics: SinkMetrics,
    /// Per-worker persist sink metrics.
    worker_metrics: SinkWorkerMetrics,
}

impl<D: Data> CorrectionV2<D> {
    /// Construct a new [`CorrectionV2`] instance.
    pub fn new(metrics: SinkMetrics, worker_metrics: SinkWorkerMetrics) -> Self {
        Self {
            chains: Default::default(),
            since: Antichain::from_elem(Timestamp::MIN),
            total_size: Default::default(),
            metrics,
            worker_metrics,
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

    /// Insert a batch of updates.
    ///
    /// All times are expected to be >= the `since`.
    fn insert_inner(&mut self, updates: &mut Vec<(D, Timestamp, Diff)>) {
        debug_assert!(updates.iter().all(|(_, t, _)| self.since.less_equal(t)));

        consolidate(updates);

        let first_update = match updates.first() {
            Some((d, t, r)) => (d, *t, *r),
            None => return,
        };

        // Optimization: If all items in `updates` sort after all items in the last chain, we can
        // append them to the last chain directly instead of constructing a new chain.
        let chain = match self.chains.last_mut() {
            Some(chain) if chain.can_accept(first_update) => chain,
            _ => {
                self.chains.push(Chain::default());
                self.chains.last_mut().unwrap()
            }
        };

        chain.extend(updates.drain(..));

        // Restore the chain invariant.
        let merge_needed = |chains: &[Chain<_>]| match chains {
            [.., prev, last] => last.len() * CHAIN_PROPORTIONALITY > prev.len(),
            _ => false,
        };

        while merge_needed(&self.chains) {
            let a = self.chains.pop().unwrap();
            let b = self.chains.pop().unwrap();
            let merged = merge_chains([a, b], &self.since);
            self.chains.push(merged);
        }

        self.update_metrics();
    }

    /// Return consolidated updates before the given `upper`.
    pub fn updates_before<'a>(
        &'a mut self,
        upper: &Antichain<Timestamp>,
    ) -> impl Iterator<Item = (D, Timestamp, Diff)> + 'a {
        let mut result = None;

        if !PartialOrder::less_than(&self.since, upper) {
            // All contained updates are beyond the upper.
            return result.into_iter().flatten();
        }

        self.consolidate_before(upper);

        // There is at most one chain that contains updates before `upper` now.
        result = self
            .chains
            .iter()
            .find(|c| c.first().is_some_and(|(_, t, _)| !upper.less_equal(&t)))
            .map(move |c| {
                let upper = upper.clone();
                c.iter().take_while(move |(_, t, _)| !upper.less_equal(t))
            });

        result.into_iter().flatten()
    }

    /// Consolidate all updates before the given `upper`.
    ///
    /// Once this method returns, all remaining updates before `upper` are contained in a single
    /// chain. Note that this chain might also contain updates beyond `upper` though!
    fn consolidate_before(&mut self, upper: &Antichain<Timestamp>) {
        if self.chains.is_empty() {
            return;
        }

        let chains = std::mem::take(&mut self.chains);
        let (merged, remains) = merge_chains_up_to(chains, &self.since, upper);

        self.chains = remains;
        if !merged.is_empty() {
            // We put the merged chain at the end, assuming that its contents are likely to
            // consolidate with retractions that will arrive soon.
            self.chains.push(merged);
        }

        // Restore the chain invariant.
        //
        // This part isn't great. We've taken great care so far to only look at updates with times
        // before `upper`, but now we might end up merging all chains anyway in the worst case.
        // There might be something smarter we could do to avoid merging as much as possible. For
        // example, we could consider sorting chains by length first, or inspect the contained
        // times and prefer merging chains that have a chance at consolidating with one another.
        let mut i = self.chains.len().saturating_sub(1);
        while i > 0 {
            let needs_merge = self.chains.get(i).is_some_and(|a| {
                let b = &self.chains[i - 1];
                a.len() * CHAIN_PROPORTIONALITY > b.len()
            });
            if needs_merge {
                let a = self.chains.remove(i);
                let b = std::mem::take(&mut self.chains[i - 1]);
                let merged = merge_chains([a, b], &self.since);
                self.chains[i - 1] = merged;
            } else {
                // Only advance the index if we didn't merge. A merge can reduce the size of the
                // chain at `i - 1`, causing an violation of the chain invariant with the next
                // chain, so we might need to merge the two before proceeding to lower indexes.
                i -= 1;
            }
        }

        self.update_metrics();
    }

    /// Advance the since frontier.
    ///
    /// # Panics
    ///
    /// Panics if the given `since` is less than the current since frontier.
    pub fn advance_since(&mut self, since: Antichain<Timestamp>) {
        assert!(PartialOrder::less_equal(&self.since, &since));
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

    /// Update persist sink metrics.
    fn update_metrics(&mut self) {
        let mut new_size = LengthAndCapacity::default();
        for chain in &mut self.chains {
            new_size += chain.get_size();
        }

        let old_size = self.total_size;
        let len_delta = UpdateDelta::new(new_size.length, old_size.length);
        let cap_delta = UpdateDelta::new(new_size.capacity, old_size.capacity);
        self.metrics
            .report_correction_update_deltas(len_delta, cap_delta);
        self.worker_metrics
            .report_correction_update_totals(new_size.length, new_size.capacity);

        self.total_size = new_size;
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
    /// Cached value of the current chain size, for efficient updating of metrics.
    cached_size: Option<LengthAndCapacity>,
}

impl<D: Data> Default for Chain<D> {
    fn default() -> Self {
        Self {
            chunks: Default::default(),
            cached_size: None,
        }
    }
}

impl<D: Data> Chain<D> {
    /// Return whether the chain is empty.
    fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    /// Return the length of the chain, in chunks.
    fn len(&self) -> usize {
        self.chunks.len()
    }

    /// Push an update onto the chain.
    ///
    /// The update must sort after all updates already in the chain, in (time, data)-order, to
    /// ensure the chain remains sorted.
    fn push<DT: Borrow<D>>(&mut self, update: (DT, Timestamp, Diff)) {
        let (d, t, r) = update;
        let update = (d.borrow(), t, r);

        debug_assert!(self.can_accept(update));

        match self.chunks.last_mut() {
            Some(c) if !c.at_capacity() => c.push(update),
            Some(_) | None => {
                let chunk = Chunk::from_update(update);
                self.push_chunk(chunk);
            }
        }

        self.invalidate_cached_size();
    }

    /// Push a chunk onto the chain.
    ///
    /// All updates in the chunk must sort after all updates already in the chain, in
    /// (time, data)-order, to ensure the chain remains sorted.
    fn push_chunk(&mut self, chunk: Chunk<D>) {
        debug_assert!(self.can_accept(chunk.first()));

        self.chunks.push(chunk);
        self.invalidate_cached_size();
    }

    /// Push the updates produced by a cursor onto the chain.
    ///
    /// All updates produced by the cursor must sort after all updates already in the chain, in
    /// (time, data)-order, to ensure the chain remains sorted.
    fn push_cursor(&mut self, cursor: Cursor<D>) {
        let mut rest = Some(cursor);
        while let Some(cursor) = rest.take() {
            let update = cursor.get();
            self.push(update);
            rest = cursor.step();
        }
    }

    /// Return whether the chain can accept the given update.
    ///
    /// A chain can accept an update if pushing it at the end upholds the (time, data)-order.
    fn can_accept(&self, update: (&D, Timestamp, Diff)) -> bool {
        self.last().is_none_or(|(dc, tc, _)| {
            let (d, t, _) = update;
            (tc, dc) < (t, d)
        })
    }

    /// Return the first update in the chain, if any.
    fn first(&self) -> Option<(&D, Timestamp, Diff)> {
        self.chunks.first().map(|c| c.first())
    }

    /// Return the last update in the chain, if any.
    fn last(&self) -> Option<(&D, Timestamp, Diff)> {
        self.chunks.last().map(|c| c.last())
    }

    /// Convert the chain into a cursor over the contained updates.
    fn into_cursor(self) -> Option<Cursor<D>> {
        let chunks = self.chunks.into_iter().map(Rc::new).collect();
        Cursor::new(chunks)
    }

    /// Return an iterator over the contained updates.
    fn iter(&self) -> impl Iterator<Item = (D, Timestamp, Diff)> + '_ {
        self.chunks
            .iter()
            .flat_map(|c| c.data.iter().map(|(d, t, r)| (d.clone(), *t, *r)))
    }

    /// Return the size of the chain, for use in metrics.
    fn get_size(&mut self) -> LengthAndCapacity {
        // This operation can be expensive as it requires inspecting the individual chunks and
        // their backing regions. We thus cache the result to hopefully avoid the cost most of the
        // time.
        if self.cached_size.is_none() {
            let mut size = LengthAndCapacity::default();
            for chunk in &mut self.chunks {
                size += chunk.get_size();
            }
            self.cached_size = Some(size);
        }

        self.cached_size.unwrap()
    }

    /// Invalidate the cached chain size.
    ///
    /// This method must be called every time the size of the chain changed.
    fn invalidate_cached_size(&mut self) {
        self.cached_size = None;
    }
}

impl<D: Data> Extend<(D, Timestamp, Diff)> for Chain<D> {
    fn extend<I: IntoIterator<Item = (D, Timestamp, Diff)>>(&mut self, iter: I) {
        for update in iter {
            self.push(update);
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
    fn get(&self) -> (&D, Timestamp, Diff) {
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

    /// Split the cursor at the given time.
    ///
    /// Returns two cursors, the first yielding all updates at times < `time`, the second yielding
    /// all updates at times >= `time`. Both can be `None` if they would be empty.
    fn split_at_time(self, time: Timestamp) -> (Option<Self>, Option<Self>) {
        let Some(skip_ts) = time.step_back() else {
            return (None, Some(self));
        };

        let before = self.clone();
        match self.skip_time(skip_ts) {
            Some((beyond, skipped)) => (before.set_limit(skipped), Some(beyond)),
            None => (Some(before), None),
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

        let mut chain = Chain::default();
        let mut remaining = Some(self);

        // We might be partway through the first chunk, in which case we can't reuse it but need to
        // allocate a new one to contain only the updates the cursor can still yield.
        while let Some(cursor) = remaining.take() {
            if cursor.chunk_offset == 0 {
                remaining = Some(cursor);
                break;
            }
            let update = cursor.get();
            chain.push(update);
            remaining = cursor.step();
        }

        if let Some(cursor) = remaining {
            for chunk in cursor.chunks {
                let chunk = Rc::into_inner(chunk).expect("checked above");
                chain.push_chunk(chunk);
            }
        }

        Ok(chain)
    }
}

impl<D: Data> From<Cursor<D>> for Chain<D> {
    fn from(cursor: Cursor<D>) -> Self {
        match cursor.try_unwrap() {
            Ok(chain) => chain,
            Err((_, cursor)) => {
                let mut chain = Chain::default();
                chain.push_cursor(cursor);
                chain
            }
        }
    }
}

/// A non-empty chunk of updates, backed by a columnation region.
///
/// All updates in a chunk are sorted by (time, data) and consolidated.
///
/// We would like all chunks to have the same fixed size, to make it easy for the allocator to
/// re-use chunk allocations. Unfortunately, the current `TimelyStack`/`ChunkedStack` API doesn't
/// provide a convenient way to pre-size regions, so chunks are currently only fixed-size in
/// spirit.
struct Chunk<D: Data> {
    /// The contained updates.
    data: StackWrapper<(D, Timestamp, Diff)>,
    /// Cached value of the current chunk size, for efficient updating of metrics.
    cached_size: Option<LengthAndCapacity>,
}

impl<D: Data> Default for Chunk<D> {
    fn default() -> Self {
        let mut data = StackWrapper::default();
        data.ensure_capacity(&mut None);

        Self {
            data,
            cached_size: None,
        }
    }
}

impl<D: Data> fmt::Debug for Chunk<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Chunk(<{}>)", self.len())
    }
}

impl<D: Data> Chunk<D> {
    /// Create a new chunk containing a single update.
    fn from_update<DT: Borrow<D>>(update: (DT, Timestamp, Diff)) -> Self {
        let (d, t, r) = update;

        let mut chunk = Self::default();
        chunk.data.copy_destructured(d.borrow(), &t, &r);

        chunk
    }

    /// Return the number of updates in the chunk.
    fn len(&self) -> usize {
        Container::len(&self.data)
    }

    /// Return whether the chunk is at capacity.
    fn at_capacity(&self) -> bool {
        self.data.at_capacity()
    }

    /// Return the update at the given index.
    ///
    /// # Panics
    ///
    /// Panics if the given index is not populated.
    fn index(&self, idx: usize) -> (&D, Timestamp, Diff) {
        let (d, t, r) = self.data.index(idx);
        (d, *t, *r)
    }

    /// Return the first update in the chunk.
    fn first(&self) -> (&D, Timestamp, Diff) {
        self.index(0)
    }

    /// Return the last update in the chunk.
    fn last(&self) -> (&D, Timestamp, Diff) {
        self.index(self.len() - 1)
    }

    /// Push an update onto the chunk.
    fn push<DT: Borrow<D>>(&mut self, update: (DT, Timestamp, Diff)) {
        let (d, t, r) = update;
        self.data.copy_destructured(d.borrow(), &t, &r);

        self.invalidate_cached_size();
    }

    /// Return the index of the first update at a time greater than `time`, or `None` if no such
    /// update exists.
    fn find_time_greater_than(&self, time: Timestamp) -> Option<usize> {
        if self.last().1 <= time {
            return None;
        }

        let mut lower = 0;
        let mut upper = self.len();
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
    fn get_size(&mut self) -> LengthAndCapacity {
        if self.cached_size.is_none() {
            let length = Container::len(&self.data);
            let mut capacity = 0;
            self.data.heap_size(|_, cap| capacity += cap);
            self.cached_size = Some(LengthAndCapacity { length, capacity });
        }

        self.cached_size.unwrap()
    }

    /// Invalidate the cached chunk size.
    ///
    /// This method must be called every time the size of the chunk changed.
    fn invalidate_cached_size(&mut self) {
        self.cached_size = None;
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
            if accum != 0 {
                updates.swap(offset, idx - 1);
                updates[offset].2 = accum;
                offset += 1;
            }
            accum = diff(&updates[idx]);
        }
    }

    if accum != 0 {
        let len = updates.len();
        updates.swap(offset, len - 1);
        updates[offset].2 = accum;
        offset += 1;
    }

    updates.truncate(offset);
}

/// Merge the given chains, advancing times by the given `since` in the process.
fn merge_chains<D: Data>(
    chains: impl IntoIterator<Item = Chain<D>>,
    since: &Antichain<Timestamp>,
) -> Chain<D> {
    let Some(&since_ts) = since.as_option() else {
        return Chain::default();
    };

    let mut to_merge = Vec::new();
    for chain in chains {
        if let Some(cursor) = chain.into_cursor() {
            let mut runs = cursor.advance_by(since_ts);
            to_merge.append(&mut runs);
        }
    }

    merge_cursors(to_merge)
}

/// Merge the given chains, advancing times by the given `since` in the process, but only up to the
/// given `upper`.
///
/// Returns the merged chain and a list of non-empty remainders of the input chains.
fn merge_chains_up_to<D: Data>(
    chains: Vec<Chain<D>>,
    since: &Antichain<Timestamp>,
    upper: &Antichain<Timestamp>,
) -> (Chain<D>, Vec<Chain<D>>) {
    let Some(&since_ts) = since.as_option() else {
        return (Chain::default(), Vec::new());
    };
    let Some(&upper_ts) = upper.as_option() else {
        let merged = merge_chains(chains, since);
        return (merged, Vec::new());
    };

    if since_ts >= upper_ts {
        // After advancing by `since` there will be no updates before `upper`.
        return (Chain::default(), chains);
    }

    let mut to_merge = Vec::new();
    let mut to_keep = Vec::new();
    for chain in chains {
        if let Some(cursor) = chain.into_cursor() {
            let mut runs = cursor.advance_by(since_ts);
            if let Some(last) = runs.pop() {
                let (before, beyond) = last.split_at_time(upper_ts);
                before.map(|c| runs.push(c));
                beyond.map(|c| to_keep.push(c));
            }
            to_merge.append(&mut runs);
        }
    }

    let merged = merge_cursors(to_merge);
    let remains = to_keep
        .into_iter()
        .map(|c| c.try_unwrap().expect("unwrapable"))
        .collect();

    (merged, remains)
}

/// Merge the given cursors into one chain.
fn merge_cursors<D: Data>(cursors: Vec<Cursor<D>>) -> Chain<D> {
    match cursors.len() {
        0 => Chain::default(),
        1 => {
            let [cur] = cursors.try_into().unwrap();
            Chain::from(cur)
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
    let mut merged = Chain::default();

    loop {
        match (rest1, rest2) {
            (Some(c1), Some(c2)) => {
                let (d1, t1, r1) = c1.get();
                let (d2, t2, r2) = c2.get();

                match (t1, d1).cmp(&(t2, d2)) {
                    Ordering::Less => {
                        merged.push((d1, t1, r1));
                        rest1 = c1.step();
                        rest2 = Some(c2);
                    }
                    Ordering::Greater => {
                        merged.push((d2, t2, r2));
                        rest1 = Some(c1);
                        rest2 = c2.step();
                    }
                    Ordering::Equal => {
                        let r = r1 + r2;
                        if r != 0 {
                            merged.push((d1, t1, r));
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

    merged
}

/// Merge the given cursors using a k-way merge with a binary heap.
fn merge_many<D: Data>(cursors: Vec<Cursor<D>>) -> Chain<D> {
    let mut heap = MergeHeap::from_iter(cursors);
    let mut merged = Chain::default();
    while let Some(cursor1) = heap.pop() {
        let (data, time, mut diff) = cursor1.get();

        while let Some((cursor2, r)) = heap.pop_equal(data, time) {
            diff += r;
            if let Some(cursor2) = cursor2.step() {
                heap.push(cursor2);
            }
        }

        if diff != 0 {
            merged.push((data, time, diff));
        }
        if let Some(cursor1) = cursor1.step() {
            heap.push(cursor1);
        }
    }

    merged
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
    fn pop_equal(&mut self, data: &D, time: Timestamp) -> Option<(Cursor<D>, Diff)> {
        let MergeCursor(cursor) = self.0.peek()?;
        let (d, t, r) = cursor.get();
        if d == data && t == time {
            let cursor = self.pop().expect("checked above");
            Some((cursor, r))
        } else {
            None
        }
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
