// Copyright 2015–2024 The Differential Dataflow contributors
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
//
// Portions of this file are derived from differential-dataflow
// (https://github.com/TimelyDataflow/differential-dataflow), licensed
// under the MIT License.

//! A `Batcher` implementation based on merge sort.
//!
//! Vendored from `differential_dataflow::trace::implementations::merge_batcher`
//! so that we can layer temporal bucketing on top of the merge batcher's
//! internal chain representation.
//!
//! The `MergeBatcher` requires support from two types, a "chunker" and a "merger".
//! The chunker receives input batches and consolidates them, producing sorted output
//! "chunks" that are fully consolidated (no adjacent updates can be accumulated).
//! The merger implements the [`Merger`] trait, and provides hooks for manipulating
//! sorted "chains" of chunks as needed by the merge batcher: merging chunks and also
//! splitting them apart based on time.

use std::marker::PhantomData;

use differential_dataflow::logging::{BatcherEvent, Logger};
use differential_dataflow::trace::{Batcher, Builder, Description};
use timely::container::{ContainerBuilder, PushInto};
use timely::progress::frontier::AntichainRef;
use timely::progress::{Timestamp, frontier::Antichain};

/// Creates batches from containers of unordered tuples.
///
/// To implement `Batcher`, the container builder `C` must accept `&mut Input` as inputs,
/// and must produce outputs of type `M::Chunk`.
pub struct MergeBatcher<Input, C, M: Merger> {
    /// Transforms input streams to chunks of sorted, consolidated data.
    chunker: C,
    /// A sequence of power-of-two length lists of sorted, consolidated containers.
    ///
    /// Do not push/pop directly but use the corresponding functions ([`Self::chain_push`]/[`Self::chain_pop`]).
    chains: Vec<Vec<M::Chunk>>,
    /// Stash of empty chunks, recycled through the merging process.
    stash: Vec<M::Chunk>,
    /// Merges consolidated chunks, and extracts the subset of an update chain that lies in an interval of time.
    merger: M,
    /// Current lower frontier, we sealed up to here.
    lower: Antichain<M::Time>,
    /// The lower-bound frontier of the data, after the last call to seal.
    frontier: Antichain<M::Time>,
    /// Logger for size accounting.
    logger: Option<Logger>,
    /// Timely operator ID.
    operator_id: usize,
    /// The `Input` type needs to be called out as the type of container accepted, but it is not otherwise present.
    _marker: PhantomData<Input>,
}

impl<Input, C, M> Batcher for MergeBatcher<Input, C, M>
where
    C: ContainerBuilder<Container = M::Chunk> + for<'a> PushInto<&'a mut Input>,
    M: Merger<Time: Timestamp>,
{
    type Input = Input;
    type Time = M::Time;
    type Output = M::Chunk;

    fn new(logger: Option<Logger>, operator_id: usize) -> Self {
        Self {
            logger,
            operator_id,
            chunker: C::default(),
            merger: M::default(),
            chains: Vec::new(),
            stash: Vec::new(),
            frontier: Antichain::new(),
            lower: Antichain::from_elem(M::Time::minimum()),
            _marker: PhantomData,
        }
    }

    /// Push a container of data into this merge batcher. Updates the internal chain structure if
    /// needed.
    fn push_container(&mut self, container: &mut Input) {
        self.chunker.push_into(container);
        while let Some(chunk) = self.chunker.extract() {
            let chunk = std::mem::take(chunk);
            self.insert_chain(vec![chunk]);
        }
    }

    // Sealing a batch means finding those updates with times not greater or equal to any time
    // in `upper`. All updates must have time greater or equal to the previously used `upper`,
    // which we call `lower`, by assumption that after sealing a batcher we receive no more
    // updates with times not greater or equal to `upper`.
    fn seal<B: Builder<Input = Self::Output, Time = Self::Time>>(
        &mut self,
        upper: Antichain<M::Time>,
    ) -> B::Output {
        // Finish
        while let Some(chunk) = self.chunker.finish() {
            let chunk = std::mem::take(chunk);
            self.insert_chain(vec![chunk]);
        }

        // Merge all remaining chains into a single chain.
        while self.chains.len() > 1 {
            let list1 = self.chain_pop().unwrap();
            let list2 = self.chain_pop().unwrap();
            let merged = self.merge_by(list1, list2);
            self.chain_push(merged);
        }
        let merged = self.chain_pop().unwrap_or_default();

        // Extract readied data.
        let mut kept = Vec::new();
        let mut readied = Vec::new();
        self.frontier.clear();

        self.merger.extract(
            merged,
            upper.borrow(),
            &mut self.frontier,
            &mut readied,
            &mut kept,
            &mut self.stash,
        );

        if !kept.is_empty() {
            self.chain_push(kept);
        }

        self.stash.clear();

        let description = Description::new(
            self.lower.clone(),
            upper.clone(),
            Antichain::from_elem(M::Time::minimum()),
        );
        let seal = B::seal(&mut readied, description);
        self.lower = upper;
        seal
    }

    /// The frontier of elements remaining after the most recent call to `self.seal`.
    #[inline]
    fn frontier(&mut self) -> AntichainRef<'_, M::Time> {
        self.frontier.borrow()
    }
}

impl<Input, C, M: Merger> MergeBatcher<Input, C, M> {
    /// Insert a chain and maintain chain properties: Chains are geometrically sized and ordered
    /// by decreasing length.
    fn insert_chain(&mut self, chain: Vec<M::Chunk>) {
        if !chain.is_empty() {
            self.chain_push(chain);
            while self.chains.len() > 1
                && (self.chains[self.chains.len() - 1].len()
                    >= self.chains[self.chains.len() - 2].len() / 2)
            {
                let list1 = self.chain_pop().unwrap();
                let list2 = self.chain_pop().unwrap();
                let merged = self.merge_by(list1, list2);
                self.chain_push(merged);
            }
        }
    }

    // merges two sorted input lists into one sorted output list.
    fn merge_by(&mut self, list1: Vec<M::Chunk>, list2: Vec<M::Chunk>) -> Vec<M::Chunk> {
        // TODO: `list1` and `list2` get dropped; would be better to reuse?
        let mut output = Vec::with_capacity(list1.len() + list2.len());
        self.merger
            .merge(list1, list2, &mut output, &mut self.stash);

        output
    }

    /// Pop a chain and account size changes.
    #[inline]
    fn chain_pop(&mut self) -> Option<Vec<M::Chunk>> {
        let chain = self.chains.pop();
        self.account(chain.iter().flatten().map(M::account), -1);
        chain
    }

    /// Push a chain and account size changes.
    #[inline]
    fn chain_push(&mut self, chain: Vec<M::Chunk>) {
        self.account(chain.iter().map(M::account), 1);
        self.chains.push(chain);
    }

    /// Account size changes. Only performs work if a logger exists.
    ///
    /// Calculate the size based on the iterator passed along, with each attribute
    /// multiplied by `diff`. Usually, one wants to pass 1 or -1 as the diff.
    #[inline]
    fn account<I: IntoIterator<Item = (usize, usize, usize, usize)>>(&self, items: I, diff: isize) {
        if let Some(logger) = &self.logger {
            let (mut records, mut size, mut capacity, mut allocations) =
                (0isize, 0isize, 0isize, 0isize);
            for (records_, size_, capacity_, allocations_) in items {
                records = records.saturating_add_unsigned(records_);
                size = size.saturating_add_unsigned(size_);
                capacity = capacity.saturating_add_unsigned(capacity_);
                allocations = allocations.saturating_add_unsigned(allocations_);
            }
            logger.log(BatcherEvent {
                operator: self.operator_id,
                records_diff: records * diff,
                size_diff: size * diff,
                capacity_diff: capacity * diff,
                allocations_diff: allocations * diff,
            })
        }
    }
}

impl<Input, C, M: Merger> Drop for MergeBatcher<Input, C, M> {
    fn drop(&mut self) {
        // Cleanup chain to retract accounting information.
        while self.chain_pop().is_some() {}
    }
}

/// A trait to describe interesting moments in a merge batcher.
pub trait Merger: Default {
    /// The internal representation of chunks of data.
    type Chunk: Default;
    /// The type of time in frontiers to extract updates.
    type Time;
    /// Merge chains into an output chain.
    fn merge(
        &mut self,
        list1: Vec<Self::Chunk>,
        list2: Vec<Self::Chunk>,
        output: &mut Vec<Self::Chunk>,
        stash: &mut Vec<Self::Chunk>,
    );
    /// Extract ready updates based on the `upper` frontier.
    fn extract(
        &mut self,
        merged: Vec<Self::Chunk>,
        upper: AntichainRef<Self::Time>,
        frontier: &mut Antichain<Self::Time>,
        readied: &mut Vec<Self::Chunk>,
        kept: &mut Vec<Self::Chunk>,
        stash: &mut Vec<Self::Chunk>,
    );

    /// Account size and allocation changes. Returns a tuple of (records, size, capacity, allocations).
    fn account(chunk: &Self::Chunk) -> (usize, usize, usize, usize);

    /// Return `(lower, upper)` antichains bracketing the times in `chunk`,
    /// or `None` if the chunk is empty or the implementation cannot
    /// produce them.
    ///
    /// `lower` is an antichain of *minimal* elements (every chunk time
    /// satisfies `lower.less_equal(t)`); `upper` is an antichain of
    /// *maximal* elements (every chunk time satisfies `t.less_equal(u)`
    /// for some `u` in `upper`). For totally-ordered times both antichains
    /// reduce to the single min / max element.
    ///
    /// Used by [`temporal::MergeBucket`] to short-circuit
    /// `Bucket::split` when the bucket's data lies entirely on one side
    /// of the split timestamp. The default returns `None`, which forces
    /// the bucket onto the merge+extract fallback.
    fn time_range(_chunk: &Self::Chunk) -> Option<(Antichain<Self::Time>, Antichain<Self::Time>)> {
        None
    }
}

pub use container::InternalMerger;

/// Build `(lower, upper)` antichains from an iterator of timestamps.
///
/// `lower` is an antichain of *minimal* elements (every emitted time is
/// in-advance-of `lower`); `upper` is an antichain of *maximal*
/// elements (every emitted time is `less_equal` to some element of
/// `upper`). For totally-ordered times both reduce to single-element
/// antichains.
pub(crate) fn build_time_bounds<T, I>(elements: I) -> (Antichain<T>, Antichain<T>)
where
    T: timely::PartialOrder + Clone,
    I: IntoIterator<Item = T>,
{
    let mut lower: Antichain<T> = Antichain::new();
    let mut max_set: Vec<T> = Vec::new();
    for t in elements {
        lower.insert(t.clone());
        if max_set.iter().any(|m| t.less_equal(m)) {
            continue;
        }
        max_set.retain(|m| !m.less_than(&t));
        max_set.push(t);
    }
    (lower, Antichain::from(max_set))
}

/// Reduce a `Vec<T>` to its maximal elements (mutually incomparable
/// elements such that no other vec element dominates them) and wrap
/// in an [`Antichain`].
pub(crate) fn reduce_to_maximal<T: timely::PartialOrder + Clone>(elements: Vec<T>) -> Antichain<T> {
    let mut max_set: Vec<T> = Vec::with_capacity(elements.len());
    for t in elements {
        if max_set.iter().any(|m| t.less_equal(m)) {
            continue;
        }
        max_set.retain(|m| !m.less_than(&t));
        max_set.push(t);
    }
    Antichain::from(max_set)
}

pub use temporal::{MergeBucket, TemporalBucketingMergeBatcher};

mod temporal {
    //! Temporal bucketing variant of [`super::MergeBatcher`].
    //!
    //! Hybrid layout: incoming data flows into a flat `chains:
    //! Vec<Vec<M::Chunk>>` exactly like plain [`super::MergeBatcher`]. On
    //! `seal`, the flat chains are merged and extracted against the seal
    //! upper; the `kept` (future-stamped) side is the only data that enters a
    //! [`BucketChain`] of [`MergeBucket`]s. Subsequent seals peel mature
    //! buckets out of the bucket chain and merge their contents with the
    //! current flat extract.
    //!
    //! For workloads where most data is current (`kept` is small at each
    //! seal) the bucket chain stays nearly empty, so per-push and per-seal
    //! cost is dominated by the plain merge-batcher path. The bucket chain
    //! only pays its splitting/merging overhead for the future tail of the
    //! input. The contract of `Batcher::seal` is preserved: only updates
    //! with time strictly less than `upper` are emitted.

    use std::marker::PhantomData;

    use differential_dataflow::logging::Logger;
    use differential_dataflow::trace::{Batcher, Builder, Description};
    use timely::container::{ContainerBuilder, PushInto};
    use timely::progress::frontier::AntichainRef;
    use timely::progress::{Antichain, Timestamp};

    use crate::merge_batcher::Merger;
    use crate::temporal::{Bucket, BucketChain, BucketTimestamp};

    /// Fuel budget used to interleave [`BucketChain::restore`] with
    /// `push_container` and `seal`.
    ///
    /// Matches the fuel used by the source-side bucketing operator until
    /// benchmarks suggest a different value.
    const RESTORE_FUEL: i64 = 1_000_000;

    /// A bucket of merge-batcher chunks for a single power-of-two time range.
    ///
    /// Maintains the same geometric chain invariant as [`super::MergeBatcher`]:
    /// adjacent chains decrease in size, merging older chains as new ones are
    /// pushed.
    ///
    /// Tracks `(lower, upper)` antichains bracketing the held times in
    /// [`Self::bounds`] (when [`Merger::time_range`] returns `Some`) so
    /// [`Bucket::split`] can short-circuit when the data lies entirely on
    /// one side of the split timestamp.
    pub struct MergeBucket<M: Merger> {
        /// Geometrically sized chains of sorted, consolidated chunks.
        chains: Vec<Vec<M::Chunk>>,
        /// `(lower, upper)` antichains over the times in [`Self::chains`].
        ///
        /// `lower` contains *minimal* elements (every held time is
        /// in-advance-of `lower`); `upper` contains *maximal* elements
        /// (every held time is `less_equal` to some element of `upper`).
        ///
        /// `None` means either the bucket is empty or the merger does not
        /// implement [`Merger::time_range`]; the latter forces
        /// [`Bucket::split`] onto its merge+extract fallback path.
        bounds: Option<(Antichain<M::Time>, Antichain<M::Time>)>,
    }

    impl<M: Merger> Default for MergeBucket<M> {
        fn default() -> Self {
            Self {
                chains: Vec::new(),
                bounds: None,
            }
        }
    }

    impl<M: Merger> MergeBucket<M>
    where
        M::Time: timely::PartialOrder + Clone,
    {
        fn is_empty(&self) -> bool {
            self.chains.iter().all(|c| c.is_empty())
        }

        /// Insert `chain` and re-establish geometric chain sizes by merging
        /// adjacent chains while their sizes are within a factor of two.
        fn insert_chain(
            &mut self,
            chain: Vec<M::Chunk>,
            merger: &mut M,
            stash: &mut Vec<M::Chunk>,
        ) {
            if chain.is_empty() {
                return;
            }
            // Union the chain's `(lower, upper)` antichains into
            // `self.bounds`. If any chunk reports `None` we drop bounds
            // to `None` so subsequent splits fall back to merge+extract.
            self.absorb_chain_bounds(&chain);
            self.chains.push(chain);
            while self.chains.len() > 1
                && self.chains[self.chains.len() - 1].len()
                    >= self.chains[self.chains.len() - 2].len() / 2
            {
                let l1 = self.chains.pop().unwrap();
                let l2 = self.chains.pop().unwrap();
                let mut out = Vec::with_capacity(l1.len() + l2.len());
                merger.merge(l1, l2, &mut out, stash);
                self.chains.push(out);
            }
        }

        /// Update `self.bounds` to reflect the union of its current value
        /// and the times in `chain`. Sets `bounds` to `None` if any
        /// chunk reports `None` (no info available).
        fn absorb_chain_bounds(&mut self, chain: &[M::Chunk]) {
            for chunk in chain {
                let Some(chunk_bounds) = M::time_range(chunk) else {
                    self.bounds = None;
                    return;
                };
                self.bounds = Some(match self.bounds.take() {
                    Some(existing) => merge_bounds(existing, chunk_bounds),
                    None => chunk_bounds,
                });
            }
        }

        /// Pairwise-merge all chains into a single sorted chain.
        ///
        /// Drops `self.bounds` since the merge consumes the chains; the
        /// caller is expected to discard the bucket or rebuild bounds if
        /// further use is needed.
        fn merge_into_one(mut self, merger: &mut M, stash: &mut Vec<M::Chunk>) -> Vec<M::Chunk> {
            while self.chains.len() > 1 {
                let l1 = self.chains.pop().unwrap();
                let l2 = self.chains.pop().unwrap();
                let mut out = Vec::with_capacity(l1.len() + l2.len());
                merger.merge(l1, l2, &mut out, stash);
                self.chains.push(out);
            }
            self.chains.pop().unwrap_or_default()
        }
    }

    impl<M> Bucket for MergeBucket<M>
    where
        M: Merger<Time: BucketTimestamp + Clone>,
    {
        type Timestamp = M::Time;

        fn split(self, timestamp: &Self::Timestamp, fuel: &mut i64) -> (Self, Self) {
            use timely::PartialOrder;

            // Short-circuit when tracked bounds tell us the data lies
            // entirely on one side of `timestamp`. This avoids the
            // O(records) merge+extract walk that the fallback path would
            // perform — the dominant cost when `BucketChain::peel` /
            // `BucketChain::restore` cascades splits over a bucket that
            // holds the whole residue (e.g. all records at a single time
            // exactly equal to a seal upper).
            //
            // The checks use timely's [`PartialOrder`] so they're correct
            // for partially-ordered timestamps too: when the antichain
            // can't be totally compared against `timestamp` neither
            // predicate fires and we fall through to the merge+extract
            // path.
            if let Some((lower, upper)) = &self.bounds {
                // All held times satisfy `t <= u` for some `u` in
                // `upper`. If every `u` in `upper` is `< timestamp` then
                // every held time is `< timestamp` → all goes to lower
                // side, upper side is empty.
                if !upper.is_empty() && upper.elements().iter().all(|u| u.less_than(timestamp)) {
                    return (self, Self::default());
                }
                // All held times satisfy `lower.less_equal(t)`. If every
                // `l` in `lower` satisfies `timestamp.less_equal(l)`
                // (i.e. `timestamp <= l`) then every held time is
                // `>= timestamp` → all goes to upper side, lower side is
                // empty.
                if !lower.is_empty() && lower.elements().iter().all(|l| timestamp.less_equal(l)) {
                    return (Self::default(), self);
                }
            }

            let had_bounds = self.bounds.is_some();
            let mut merger = M::default();
            let mut stash = Vec::new();
            let merged = self.merge_into_one(&mut merger, &mut stash);

            // Account fuel by total record count of the chunks we are about to
            // partition. `M::account(...).0` is the record count.
            let records: usize = merged.iter().map(|c| M::account(c).0).sum();
            *fuel = fuel.saturating_sub(i64::try_from(records).unwrap_or(i64::MAX));

            let upper_at = Antichain::from_elem(timestamp.clone());
            let mut frontier = Antichain::new();
            let mut ship = Vec::new();
            let mut keep = Vec::new();
            merger.extract(
                merged,
                upper_at.borrow(),
                &mut frontier,
                &mut ship,
                &mut keep,
                &mut stash,
            );
            // Recompute child bounds from the freshly-extracted chunks.
            // If the merger doesn't implement `time_range`, both sides
            // fall back to `None` (and future splits stay on the
            // fallback path).
            let lower_bounds = if had_bounds {
                bounds_from_chain::<M>(&ship)
            } else {
                None
            };
            let upper_bounds = if had_bounds {
                bounds_from_chain::<M>(&keep)
            } else {
                None
            };
            let lower = MergeBucket {
                chains: if ship.is_empty() {
                    Vec::new()
                } else {
                    vec![ship]
                },
                bounds: lower_bounds,
            };
            let upper = MergeBucket {
                chains: if keep.is_empty() {
                    Vec::new()
                } else {
                    vec![keep]
                },
                bounds: upper_bounds,
            };
            (lower, upper)
        }
    }

    /// Compute `(lower, upper)` antichains over a chain by walking each
    /// chunk's [`Merger::time_range`]. Returns `None` if the chain is
    /// empty or any chunk reports `None`.
    fn bounds_from_chain<M: Merger<Time: timely::PartialOrder + Clone>>(
        chain: &[M::Chunk],
    ) -> Option<(Antichain<M::Time>, Antichain<M::Time>)> {
        let mut bounds: Option<(Antichain<M::Time>, Antichain<M::Time>)> = None;
        for chunk in chain {
            let chunk_bounds = M::time_range(chunk)?;
            bounds = Some(match bounds.take() {
                Some(existing) => merge_bounds(existing, chunk_bounds),
                None => chunk_bounds,
            });
        }
        bounds
    }

    /// Union two `(lower, upper)` antichain pairs.
    ///
    /// `lower` is an antichain of minimal elements; the union retains
    /// only minimal elements from the combined set, exactly what
    /// [`Antichain::insert`] does. `upper` is an antichain of maximal
    /// elements; the union retains only maximal elements, computed via
    /// [`reduce_to_maximal`].
    fn merge_bounds<T: timely::PartialOrder + Clone>(
        a: (Antichain<T>, Antichain<T>),
        b: (Antichain<T>, Antichain<T>),
    ) -> (Antichain<T>, Antichain<T>) {
        let (a_lower, a_upper) = a;
        let (b_lower, b_upper) = b;
        let mut lower = a_lower;
        for elt in b_lower.into_iter() {
            lower.insert(elt);
        }
        let upper_elements: Vec<T> = a_upper.into_iter().chain(b_upper).collect();
        let upper = super::reduce_to_maximal(upper_elements);
        (lower, upper)
    }

    /// A merge batcher that holds back future-stamped updates using a
    /// [`BucketChain`], while routing all incoming data through plain
    /// merge-batcher chains.
    ///
    /// Outer shape mirrors [`super::MergeBatcher`]: a chunker `C` produces
    /// [`Merger::Chunk`]s, a merger `M` merges and extracts them. Internally,
    /// the flat `chains: Vec<Vec<M::Chunk>>` field — identical to plain
    /// `MergeBatcher` — receives every input chunk. A separate
    /// [`BucketChain`] holds only the `kept` (future-stamped) tail produced
    /// by seal-time extracts, so the bucketing machinery does no work for
    /// data that is already at or below the current frontier.
    pub struct TemporalBucketingMergeBatcher<Input, C, M>
    where
        M: Merger<Time: BucketTimestamp>,
    {
        chunker: C,
        /// Flat geometric chains, mirroring plain [`super::MergeBatcher`].
        /// Every input chunk lands here.
        chains: Vec<Vec<M::Chunk>>,
        /// Bucket chain holding only future-stamped data deposited by prior
        /// seals' `kept` extracts.
        bucket_chain: BucketChain<MergeBucket<M>>,
        /// Total record count currently held in [`Self::bucket_chain`].
        ///
        /// Maintained alongside insertions and peel-time drains so `seal`
        /// can take a fast path that skips all bucket-chain work whenever
        /// no future-stamped data has accumulated. For non-temporal
        /// workloads this counter stays at `0` and the per-seal cost
        /// matches plain [`super::MergeBatcher`].
        held_records: usize,
        stash: Vec<M::Chunk>,
        merger: M,
        lower: Antichain<M::Time>,
        frontier: Antichain<M::Time>,
        #[allow(dead_code)]
        logger: Option<Logger>,
        #[allow(dead_code)]
        operator_id: usize,
        _marker: PhantomData<Input>,
    }

    impl<Input, C, M> TemporalBucketingMergeBatcher<Input, C, M>
    where
        C: ContainerBuilder<Container = M::Chunk>,
        M: Merger<Time: Timestamp + BucketTimestamp + Clone>,
    {
        /// Insert a chain into the flat path with geometric merging,
        /// matching plain [`super::MergeBatcher::insert_chain`].
        fn insert_chain(&mut self, chain: Vec<M::Chunk>) {
            if chain.is_empty() {
                return;
            }
            self.chains.push(chain);
            while self.chains.len() > 1
                && self.chains[self.chains.len() - 1].len()
                    >= self.chains[self.chains.len() - 2].len() / 2
            {
                let l1 = self.chains.pop().unwrap();
                let l2 = self.chains.pop().unwrap();
                let mut out = Vec::with_capacity(l1.len() + l2.len());
                self.merger.merge(l1, l2, &mut out, &mut self.stash);
                self.chains.push(out);
            }
        }

        /// Route an entire sorted, consolidated chain into
        /// [`Self::bucket_chain`].
        ///
        /// The whole chain is fed as a unit — it is already a
        /// geometric-merge-ready level, so the destination bucket can
        /// absorb it without per-chunk pairwise merging. Routing per
        /// chunk would force the bucket to rebuild the chain via
        /// O(records × log chunks) merges.
        ///
        /// Fast path: when the chain's combined `(lower, upper)` time
        /// bounds all map to a single bucket, push the chain into that
        /// bucket directly. No `M::extract`.
        ///
        /// Slow path (straddle or unknown bounds): drive a frontier-led
        /// loop that jumps to the bucket containing the next remaining
        /// time, extracts that bucket's portion, and ships it. The
        /// extract output frontier (the lower bound of `keep`) selects
        /// the next bucket, so empty intermediate buckets are skipped
        /// instead of paying an `O(records)` extract per boundary.
        /// [`BucketChain::peel`] correctness depends on each bucket
        /// only holding data in its declared range — peeled buckets
        /// are emitted as ready without further extract, so a bucket
        /// must not contain data with `t >= bucket_end`.
        ///
        /// Used at seal time for `kept` chains; never on the input path.
        fn route_chain_into_bucket_chain(&mut self, chain: Vec<M::Chunk>) {
            if chain.is_empty() {
                return;
            }
            let total_records: usize = chain.iter().map(|c| M::account(c).0).sum();
            self.held_records = self.held_records.saturating_add(total_records);

            if self.bucket_chain.is_empty() {
                // Chain was fully drained by a prior peel. Reseed with a
                // single full-domain bucket and slot the chain there.
                self.bucket_chain = BucketChain::new(MergeBucket::default());
                let bucket = self
                    .bucket_chain
                    .find_mut(&M::Time::minimum())
                    .expect("freshly seeded chain has a bucket");
                bucket.insert_chain(chain, &mut self.merger, &mut self.stash);
                return;
            }

            // Fast path: union the chain's chunk bounds and check whether
            // they all live in a single bucket.
            let bounds = bounds_from_chain::<M>(&chain);
            let target_start = bounds.as_ref().and_then(|(lower, upper)| {
                let min_t = lower.elements().first()?;
                let target = self.bucket_chain.range_of(min_t)?.start;
                let all_fit = upper.elements().iter().all(|u| {
                    self.bucket_chain
                        .range_of(u)
                        .is_some_and(|r| r.start == target)
                });
                all_fit.then_some(target)
            });
            if let Some(start) = target_start {
                let bucket = self
                    .bucket_chain
                    .find_mut(&start)
                    .expect("bucket exists for known time");
                bucket.insert_chain(chain, &mut self.merger, &mut self.stash);
                return;
            }

            // Slow path: walk bucket starts with a monotonically
            // advancing cursor, driven by the frontier returned from
            // each `extract`. Seed with the chain's combined lower
            // bound when known so we land on the first populated
            // bucket immediately; otherwise start at the chain head
            // and let the first extract supply the real frontier.
            //
            // Snapshot starts; we can't hold an immutable iterator
            // across the `find_mut` borrow that follows each extract.
            let starts: Vec<M::Time> = self.bucket_chain.starts().cloned().collect();
            let mut bucket_idx: usize = 0;
            let mut scratch_frontier = match bounds {
                Some((lower, _)) => lower,
                None => Antichain::from_elem(M::Time::minimum()),
            };
            let mut current = chain;
            while !scratch_frontier.is_empty() && !current.is_empty() {
                let next_t = scratch_frontier
                    .elements()
                    .first()
                    .expect("non-empty antichain")
                    .clone();
                // Advance the cursor until `starts[bucket_idx] <=
                // next_t < starts[bucket_idx + 1]`. The total advance
                // across all iterations is bounded by `starts.len()`,
                // so empty intermediate buckets are skipped without
                // paying an `extract` per boundary.
                while bucket_idx + 1 < starts.len() && starts[bucket_idx + 1] <= next_t {
                    bucket_idx += 1;
                }
                let bucket_start = starts[bucket_idx].clone();
                // Exclusive upper is the next bucket's start; the
                // unbounded last bucket has none, signaled to
                // `extract` by an empty antichain.
                let upper = match starts.get(bucket_idx + 1) {
                    Some(end) => Antichain::from_elem(end.clone()),
                    None => Antichain::new(),
                };
                scratch_frontier.clear();
                let mut ship = Vec::new();
                let mut keep = Vec::new();
                self.merger.extract(
                    std::mem::take(&mut current),
                    upper.borrow(),
                    &mut scratch_frontier,
                    &mut ship,
                    &mut keep,
                    &mut self.stash,
                );
                if !ship.is_empty() {
                    let bucket = self
                        .bucket_chain
                        .find_mut(&bucket_start)
                        .expect("bucket exists for known start");
                    bucket.insert_chain(ship, &mut self.merger, &mut self.stash);
                }
                current = keep;
            }
        }

        /// Recompute `self.frontier` as the held-data lower bound, walking
        /// only [`Self::bucket_chain`] (the only place data is held after a
        /// seal — flat chains are emptied during the seal extract).
        ///
        /// A bucket's start is only a range lower bound; to produce the
        /// actual minimum we merge the chains in the lowest non-empty bucket
        /// and use `Merger::extract` against the bucket start so every entry
        /// is classified as `keep`, which is the side that updates the
        /// frontier antichain. The merged chain replaces the bucket's chains
        /// so subsequent seals pay only the linear extract cost.
        fn recompute_frontier(&mut self) {
            self.frontier.clear();
            // Walk buckets in order, taking chains out of the first non-empty one.
            // Avoids the per-seal `Vec` allocation and per-bucket `find_mut` cost
            // that an outer loop driven by cloned starts would incur.
            let mut found: Option<(M::Time, Vec<Vec<M::Chunk>>)> = None;
            for (start, bucket) in self.bucket_chain.iter_mut() {
                if !bucket.is_empty() {
                    found = Some((start.clone(), std::mem::take(&mut bucket.chains)));
                    break;
                }
            }
            let Some((start, chains)) = found else { return };

            let merged = MergeBucket::<M> {
                chains,
                bounds: None,
            }
            .merge_into_one(&mut self.merger, &mut self.stash);
            let upper = Antichain::from_elem(start.clone());
            let mut ship = Vec::new();
            let mut keep = Vec::new();
            self.merger.extract(
                merged,
                upper.borrow(),
                &mut self.frontier,
                &mut ship,
                &mut keep,
                &mut self.stash,
            );
            debug_assert!(
                ship.is_empty(),
                "all times in a bucket must be >= the bucket's start"
            );
            let bucket = self
                .bucket_chain
                .find_mut(&start)
                .expect("bucket still exists");
            if !keep.is_empty() {
                bucket.chains.push(keep);
            }
        }
    }

    impl<Input, C, M> Batcher for TemporalBucketingMergeBatcher<Input, C, M>
    where
        C: ContainerBuilder<Container = M::Chunk> + for<'a> PushInto<&'a mut Input>,
        M: Merger<Time: Timestamp + BucketTimestamp + Clone>,
    {
        type Input = Input;
        type Time = M::Time;
        type Output = M::Chunk;

        fn new(logger: Option<Logger>, operator_id: usize) -> Self {
            Self {
                logger,
                operator_id,
                chunker: C::default(),
                merger: M::default(),
                chains: Vec::new(),
                bucket_chain: BucketChain::new(MergeBucket::default()),
                held_records: 0,
                stash: Vec::new(),
                frontier: Antichain::new(),
                lower: Antichain::from_elem(M::Time::minimum()),
                _marker: PhantomData,
            }
        }

        fn push_container(&mut self, container: &mut Input) {
            self.chunker.push_into(container);
            while let Some(chunk) = self.chunker.extract() {
                let chunk = std::mem::take(chunk);
                self.insert_chain(vec![chunk]);
            }
        }

        fn seal<B: Builder<Input = Self::Output, Time = Self::Time>>(
            &mut self,
            upper: Antichain<M::Time>,
        ) -> B::Output {
            // Drain the chunker into the flat path.
            while let Some(chunk) = self.chunker.finish() {
                let chunk = std::mem::take(chunk);
                self.insert_chain(vec![chunk]);
            }

            // Merge all flat chains into one sorted chain.
            while self.chains.len() > 1 {
                let l1 = self.chains.pop().unwrap();
                let l2 = self.chains.pop().unwrap();
                let mut out = Vec::with_capacity(l1.len() + l2.len());
                self.merger.merge(l1, l2, &mut out, &mut self.stash);
                self.chains.push(out);
            }
            let merged_flat = self.chains.pop().unwrap_or_default();

            // Extract from the flat side. `flat_ready` has `t < upper`,
            // `kept` has `t >= upper` and feeds the bucket chain. We use a
            // scratch frontier because `self.frontier` is recomputed below.
            let mut flat_ready: Vec<M::Chunk> = Vec::new();
            let mut kept = Vec::new();
            let mut scratch_frontier = Antichain::new();
            self.merger.extract(
                merged_flat,
                upper.borrow(),
                &mut scratch_frontier,
                &mut flat_ready,
                &mut kept,
                &mut self.stash,
            );

            // Diagnostic counters; cheap to collect, gated to DEBUG below.
            let trace_enabled = tracing::enabled!(tracing::Level::DEBUG);
            let flat_ready_records: usize = if trace_enabled {
                flat_ready.iter().map(|c| M::account(c).0).sum()
            } else {
                0
            };
            let kept_records: usize = if trace_enabled {
                kept.iter().map(|c| M::account(c).0).sum()
            } else {
                0
            };
            let held_before = self.held_records;

            // Fast path: no held data and nothing new to hold. Skip the
            // bucket-chain bookkeeping (peel, merge, restore, frontier
            // recomputation) so the per-seal cost matches the plain
            // [`super::MergeBatcher`].
            let mut peel_records: usize = 0;
            let mut peeled_buckets: usize = 0;
            let fast_path = self.held_records == 0 && kept.is_empty();
            let mut readied = if fast_path {
                self.frontier.clear();
                flat_ready
            } else {
                // Future-stamped data enters the bucket chain. Route
                // the whole chain at once so the destination bucket can
                // adopt it as a single geometric level rather than
                // pairwise-merging it back into existence.
                self.route_chain_into_bucket_chain(kept);

                // Peel buckets fully below `upper`. The peel may split the
                // boundary-crossing bucket; the upper half is reinserted.
                let peeled = self.bucket_chain.peel(upper.borrow());

                // Merge each peeled bucket into a single sorted chain, then
                // merge all of those into one running chain. Account peeled
                // records as leaving the bucket chain.
                let mut peeled_chain: Vec<M::Chunk> = Vec::new();
                for bucket in peeled {
                    let bucket_one = bucket.merge_into_one(&mut self.merger, &mut self.stash);
                    if bucket_one.is_empty() {
                        continue;
                    }
                    let drained: usize = bucket_one.iter().map(|c| M::account(c).0).sum();
                    self.held_records = self.held_records.saturating_sub(drained);
                    peel_records = peel_records.saturating_add(drained);
                    peeled_buckets = peeled_buckets.saturating_add(1);
                    if peeled_chain.is_empty() {
                        peeled_chain = bucket_one;
                    } else {
                        let mut out = Vec::with_capacity(peeled_chain.len() + bucket_one.len());
                        self.merger
                            .merge(peeled_chain, bucket_one, &mut out, &mut self.stash);
                        peeled_chain = out;
                    }
                }

                // Combine flat-side ready chunks with peeled-side chunks into a
                // single sorted chain. Peeled buckets are entirely below `upper`
                // by `peel`'s contract, so no further extract is needed.
                let combined = if flat_ready.is_empty() {
                    peeled_chain
                } else if peeled_chain.is_empty() {
                    flat_ready
                } else {
                    let mut out = Vec::with_capacity(flat_ready.len() + peeled_chain.len());
                    self.merger
                        .merge(flat_ready, peeled_chain, &mut out, &mut self.stash);
                    out
                };

                // Maintain bucket-chain shape with bounded fuel.
                let mut fuel = RESTORE_FUEL;
                self.bucket_chain.restore(&mut fuel);

                // Recompute the held-data frontier from the lowest non-empty
                // bucket. After the seal, all data is in `bucket_chain` (the
                // flat chains have been drained). For totally-ordered timestamps
                // — the only kind that impl `BucketTimestamp` — this produces a
                // single-element antichain at the smallest held time.
                self.recompute_frontier();

                combined
            };

            self.stash.clear();

            tracing::debug!(
                target: "mz_timely_util::merge_batcher::temporal",
                operator_id = self.operator_id,
                upper = ?upper.borrow(),
                lower = ?self.lower.borrow(),
                frontier_after = ?self.frontier.borrow(),
                flat_ready_records,
                kept_records,
                held_before,
                held_after = self.held_records,
                peeled_buckets,
                peel_records,
                fast_path,
                bucket_count = self.bucket_chain.len(),
                "seal",
            );

            let description = Description::new(
                self.lower.clone(),
                upper.clone(),
                Antichain::from_elem(M::Time::minimum()),
            );
            let seal = B::seal(&mut readied, description);
            self.lower = upper;
            seal
        }

        fn frontier(&mut self) -> AntichainRef<'_, M::Time> {
            self.frontier.borrow()
        }
    }

    impl<Input, C, M> TemporalBucketingMergeBatcher<Input, C, M>
    where
        M: Merger<Time: BucketTimestamp>,
    {
        /// Returns true if both the flat chains and every bucket are empty.
        #[cfg(test)]
        pub(super) fn is_chain_empty(&self) -> bool {
            if !self.chains.is_empty() {
                return false;
            }
            let starts: Vec<_> = self.bucket_chain.starts().cloned().collect();
            starts.iter().all(|s| {
                self.bucket_chain
                    .find(s)
                    .map(|b| b.is_empty())
                    .unwrap_or(true)
            })
        }
    }

    #[cfg(test)]
    mod tests {
        use std::marker::PhantomData;

        use differential_dataflow::trace::implementations::chunker::ContainerChunker;
        use differential_dataflow::trace::{Batcher, Builder, Description};
        use timely::progress::{Antichain, Timestamp};

        use crate::merge_batcher::TemporalBucketingMergeBatcher;
        use crate::merge_batcher::container::VecMerger;

        type TestTime = u64;
        type TestUpdate = (u64, TestTime, i64);
        type TestBatcher = TemporalBucketingMergeBatcher<
            Vec<TestUpdate>,
            ContainerChunker<Vec<TestUpdate>>,
            VecMerger<u64, TestTime, i64>,
        >;

        /// A `Builder` that collects sealed chunks into a flat `Vec<TestUpdate>`.
        struct VecCapturingBuilder<D, T>(PhantomData<(D, T)>);

        impl<D: Clone + 'static, T: Timestamp> Builder for VecCapturingBuilder<D, T> {
            type Input = Vec<D>;
            type Time = T;
            type Output = Vec<D>;

            fn with_capacity(_keys: usize, _vals: usize, _upds: usize) -> Self {
                unimplemented!()
            }
            fn push(&mut self, _chunk: &mut Self::Input) {
                unimplemented!()
            }
            fn done(self, _description: Description<Self::Time>) -> Self::Output {
                unimplemented!()
            }

            fn seal(
                chain: &mut Vec<Self::Input>,
                _description: Description<Self::Time>,
            ) -> Self::Output {
                let mut out = Vec::new();
                for mut chunk in std::mem::take(chain) {
                    out.append(&mut chunk);
                }
                out
            }
        }

        fn push(batcher: &mut TestBatcher, mut updates: Vec<TestUpdate>) {
            batcher.push_container(&mut updates);
        }

        fn seal(batcher: &mut TestBatcher, upper: Option<TestTime>) -> Vec<TestUpdate> {
            let antichain = match upper {
                Some(u) => Antichain::from_elem(u),
                None => Antichain::new(),
            };
            batcher.seal::<VecCapturingBuilder<TestUpdate, TestTime>>(antichain)
        }

        #[mz_ore::test]
        fn distinct_timestamps_stay_distinct() {
            let mut batcher = TestBatcher::new(None, 0);
            let input: Vec<TestUpdate> = (0_u64..32).map(|i| (i, i, 1_i64)).collect();
            push(&mut batcher, input.clone());

            let drained = seal(&mut batcher, None);
            let mut sorted_drained = drained.clone();
            sorted_drained.sort();
            let mut sorted_input = input;
            sorted_input.sort();
            assert_eq!(sorted_drained, sorted_input);
        }

        #[mz_ore::test]
        fn seal_returns_strictly_less_than_upper() {
            let mut batcher = TestBatcher::new(None, 0);
            let input: Vec<TestUpdate> = (0_u64..16).map(|i| (i, i, 1_i64)).collect();
            push(&mut batcher, input);

            let early = seal(&mut batcher, Some(8));
            for (_, t, _) in &early {
                assert!(*t < 8, "seal returned t={t} not strictly less than 8");
            }
            // Updates with t in [0, 8) must all be present.
            let mut early_times: Vec<TestTime> = early.iter().map(|(_, t, _)| *t).collect();
            early_times.sort();
            assert_eq!(early_times, (0_u64..8).collect::<Vec<_>>());

            let rest = seal(&mut batcher, None);
            let mut rest_times: Vec<TestTime> = rest.iter().map(|(_, t, _)| *t).collect();
            rest_times.sort();
            assert_eq!(rest_times, (8_u64..16).collect::<Vec<_>>());

            assert!(batcher.is_chain_empty());
        }

        #[mz_ore::test]
        fn frontier_reports_held_lower_bound() {
            let mut batcher = TestBatcher::new(None, 0);
            let input: Vec<TestUpdate> = vec![(1, 3, 1), (2, 5, 1), (3, 7, 1), (4, 11, 1)];
            push(&mut batcher, input);

            // Seal up to 4. Held data is {5, 7, 11}; the smallest is 5.
            // The frontier reflects the actual minimum held time, not the
            // surrounding bucket's start.
            let _ = seal(&mut batcher, Some(4));
            assert_eq!(&*batcher.frontier(), &[5_u64]);

            // Drain everything.
            let _ = seal(&mut batcher, None);
            assert!(batcher.frontier().is_empty());
        }

        #[mz_ore::test]
        fn frontier_is_actual_min_not_bucket_start() {
            // The smallest held time is 1_000_000, well above any low bucket
            // boundary. The frontier should reflect that exact value, not the
            // power-of-two bucket boundary that contains it.
            let mut batcher = TestBatcher::new(None, 0);
            let input: Vec<TestUpdate> =
                (1_000_000_u64..1_000_010).map(|i| (i, i, 1_i64)).collect();
            push(&mut batcher, input);

            let _ = seal(&mut batcher, Some(500));
            assert_eq!(&*batcher.frontier(), &[1_000_000_u64]);
        }

        #[mz_ore::test]
        fn future_stamped_updates_are_held_back() {
            let mut batcher = TestBatcher::new(None, 0);
            // All updates at t >= 100, but seal upper is 50 — nothing should be released.
            let input: Vec<TestUpdate> = (100_u64..120).map(|i| (i, i, 1_i64)).collect();
            push(&mut batcher, input.clone());

            let early = seal(&mut batcher, Some(50));
            assert!(early.is_empty(), "no data with t < 50 should be released");

            // After draining, all updates show up.
            let drained = seal(&mut batcher, None);
            let mut sorted = drained.clone();
            sorted.sort();
            let mut input_sorted = input;
            input_sorted.sort();
            assert_eq!(sorted, input_sorted);
        }
    }
}

pub mod container {
    //! Merger implementations for the merge batcher.
    //!
    //! The `InternalMerge` trait allows containers to merge sorted, consolidated
    //! data using internal iteration. The `InternalMerger` type implements the
    //! `Merger` trait using `InternalMerge`, and is the standard merger for all
    //! container types.

    use std::marker::PhantomData;
    use timely::container::SizableContainer;
    use timely::progress::frontier::{Antichain, AntichainRef};
    use timely::{Accountable, PartialOrder};

    use crate::merge_batcher::Merger;

    /// A container that supports the operations needed by the merge batcher:
    /// merging sorted chains and extracting updates by time.
    pub trait InternalMerge: Accountable + SizableContainer + Default {
        /// The owned time type, for maintaining antichains.
        type TimeOwned;

        /// The number of items in this container.
        fn len(&self) -> usize;

        /// Clear the container for reuse.
        fn clear(&mut self);

        /// Account the allocations behind the chunk.
        fn account(&self) -> (usize, usize, usize, usize) {
            let (size, capacity, allocations) = (0, 0, 0);
            (
                usize::try_from(self.record_count()).unwrap(),
                size,
                capacity,
                allocations,
            )
        }

        /// Merge items from sorted inputs into `self`, advancing positions.
        /// Merges until `self` is at capacity or all inputs are exhausted.
        ///
        /// Dispatches based on the number of inputs:
        /// - **0**: no-op
        /// - **1**: bulk copy (may swap the input into `self`)
        /// - **2**: merge two sorted streams
        fn merge_from(&mut self, others: &mut [Self], positions: &mut [usize]);

        /// Extract updates from `self` into `ship` (times not beyond `upper`)
        /// and `keep` (times beyond `upper`), updating `frontier` with kept times.
        ///
        /// Iteration starts at `*position` and advances `*position` as updates
        /// are consumed. The implementation must yield (return early) when
        /// either `keep.at_capacity()` or `ship.at_capacity()` becomes true,
        /// so the caller can swap out a full output buffer and resume by
        /// calling `extract` again. The caller invokes `extract` repeatedly
        /// until `*position >= self.len()`.
        ///
        /// This shape exists because `at_capacity()` for `Vec` is
        /// `len() == capacity()`, which silently becomes false again the
        /// moment a push past capacity grows the backing allocation.
        /// Without per-element yielding, a single `extract` call can
        /// quietly produce oversized output chunks.
        fn extract(
            &mut self,
            position: &mut usize,
            upper: AntichainRef<Self::TimeOwned>,
            frontier: &mut Antichain<Self::TimeOwned>,
            keep: &mut Self,
            ship: &mut Self,
        );

        /// Return `(lower, upper)` antichains bracketing the times in
        /// `self`, or `None` if the container is empty / the impl can't
        /// answer. See [`Merger::time_range`] for the contract.
        fn time_range(&self) -> Option<(Antichain<Self::TimeOwned>, Antichain<Self::TimeOwned>)> {
            None
        }
    }

    /// A `Merger` for `Vec` containers, which contain owned data and need special treatment.
    pub type VecInternalMerger<D, T, R> = VecMerger<D, T, R>;
    /// A `Merger` implementation for `Vec<(D, T, R)>` that drains owned inputs.
    pub struct VecMerger<D, T, R> {
        _marker: PhantomData<(D, T, R)>,
    }

    impl<D, T, R> Default for VecMerger<D, T, R> {
        fn default() -> Self {
            Self {
                _marker: PhantomData,
            }
        }
    }

    impl<D, T, R> VecMerger<D, T, R> {
        /// The target capacity for output buffers, as a power of two.
        ///
        /// This amount is used to size vectors, where vectors not exactly this capacity are dropped.
        /// If this is mis-set, there is the potential for more memory churn than anticipated.
        fn target_capacity() -> usize {
            timely::container::buffer::default_capacity::<(D, T, R)>().next_power_of_two()
        }
        /// Acquire a buffer with the target capacity.
        fn empty(&self, stash: &mut Vec<Vec<(D, T, R)>>) -> Vec<(D, T, R)> {
            let target = Self::target_capacity();
            let mut container = stash.pop().unwrap_or_default();
            container.clear();
            // Reuse if at target; otherwise allocate fresh.
            if container.capacity() != target {
                container = Vec::with_capacity(target);
            }
            container
        }
        /// Refill `queue` from `iter` if empty. Recycles drained queues into `stash`.
        fn refill(
            queue: &mut std::collections::VecDeque<(D, T, R)>,
            iter: &mut impl Iterator<Item = Vec<(D, T, R)>>,
            stash: &mut Vec<Vec<(D, T, R)>>,
        ) {
            if queue.is_empty() {
                let target = Self::target_capacity();
                if stash.len() < 2 {
                    let mut recycled = Vec::from(std::mem::take(queue));
                    recycled.clear();
                    if recycled.capacity() == target {
                        stash.push(recycled);
                    }
                }
                if let Some(chunk) = iter.next() {
                    *queue = std::collections::VecDeque::from(chunk);
                }
            }
        }
    }

    impl<D, T, R> Merger for VecMerger<D, T, R>
    where
        D: Ord + Clone + 'static,
        T: Ord + Clone + PartialOrder + 'static,
        R: differential_dataflow::difference::Semigroup + Clone + 'static,
    {
        type Chunk = Vec<(D, T, R)>;
        type Time = T;

        fn merge(
            &mut self,
            list1: Vec<Vec<(D, T, R)>>,
            list2: Vec<Vec<(D, T, R)>>,
            output: &mut Vec<Vec<(D, T, R)>>,
            stash: &mut Vec<Vec<(D, T, R)>>,
        ) {
            use std::cmp::Ordering;
            use std::collections::VecDeque;

            let mut iter1 = list1.into_iter();
            let mut iter2 = list2.into_iter();
            let mut q1 = VecDeque::<(D, T, R)>::from(iter1.next().unwrap_or_default());
            let mut q2 = VecDeque::<(D, T, R)>::from(iter2.next().unwrap_or_default());

            let mut result = self.empty(stash);

            // Merge while both queues are non-empty.
            while let (Some((d1, t1, _)), Some((d2, t2, _))) = (q1.front(), q2.front()) {
                match (d1, t1).cmp(&(d2, t2)) {
                    Ordering::Less => {
                        result.push(q1.pop_front().unwrap());
                    }
                    Ordering::Greater => {
                        result.push(q2.pop_front().unwrap());
                    }
                    Ordering::Equal => {
                        let (d, t, mut r1) = q1.pop_front().unwrap();
                        let (_, _, r2) = q2.pop_front().unwrap();
                        r1.plus_equals(&r2);
                        if !r1.is_zero() {
                            result.push((d, t, r1));
                        }
                    }
                }

                if result.at_capacity() {
                    output.push(std::mem::take(&mut result));
                    result = self.empty(stash);
                }

                // Refill emptied queues from their chains.
                if q1.is_empty() {
                    Self::refill(&mut q1, &mut iter1, stash);
                }
                if q2.is_empty() {
                    Self::refill(&mut q2, &mut iter2, stash);
                }
            }

            // Push partial result and remaining data from both sides.
            if !result.is_empty() {
                output.push(result);
            }
            for q in [q1, q2] {
                if !q.is_empty() {
                    output.push(Vec::from(q));
                }
            }
            output.extend(iter1);
            output.extend(iter2);
        }

        fn extract(
            &mut self,
            merged: Vec<Vec<(D, T, R)>>,
            upper: AntichainRef<T>,
            frontier: &mut Antichain<T>,
            ship: &mut Vec<Vec<(D, T, R)>>,
            kept: &mut Vec<Vec<(D, T, R)>>,
            stash: &mut Vec<Vec<(D, T, R)>>,
        ) {
            let mut keep = self.empty(stash);
            let mut ready = self.empty(stash);

            for mut chunk in merged {
                // Go update-by-update to swap out full containers.
                for (data, time, diff) in chunk.drain(..) {
                    if upper.less_equal(&time) {
                        frontier.insert_with(&time, |time| time.clone());
                        keep.push((data, time, diff));
                    } else {
                        ready.push((data, time, diff));
                    }
                    if keep.at_capacity() {
                        kept.push(std::mem::take(&mut keep));
                        keep = self.empty(stash);
                    }
                    if ready.at_capacity() {
                        ship.push(std::mem::take(&mut ready));
                        ready = self.empty(stash);
                    }
                }
                // Recycle the now-empty chunk if it has the right capacity.
                if chunk.capacity() == Self::target_capacity() {
                    stash.push(chunk);
                }
            }
            if !keep.is_empty() {
                kept.push(keep);
            }
            if !ready.is_empty() {
                ship.push(ready);
            }
        }

        fn account(chunk: &Vec<(D, T, R)>) -> (usize, usize, usize, usize) {
            (chunk.len(), 0, 0, 0)
        }

        fn time_range(chunk: &Vec<(D, T, R)>) -> Option<(Antichain<T>, Antichain<T>)> {
            if chunk.is_empty() {
                return None;
            }
            Some(super::build_time_bounds(
                chunk.iter().map(|(_, t, _)| t.clone()),
            ))
        }
    }

    /// A merger that uses internal iteration via [`InternalMerge`].
    pub struct InternalMerger<MC> {
        _marker: PhantomData<MC>,
    }

    impl<MC> Default for InternalMerger<MC> {
        fn default() -> Self {
            Self {
                _marker: PhantomData,
            }
        }
    }

    impl<MC> InternalMerger<MC>
    where
        MC: InternalMerge,
    {
        #[inline]
        fn empty(&self, stash: &mut Vec<MC>) -> MC {
            stash.pop().unwrap_or_else(|| {
                let mut container = MC::default();
                container.ensure_capacity(&mut None);
                container
            })
        }
        #[inline]
        fn recycle(&self, mut chunk: MC, stash: &mut Vec<MC>) {
            chunk.clear();
            stash.push(chunk);
        }
        /// Drain remaining items from one side into `result`/`output`.
        ///
        /// Copies the partially-consumed head into `result`, then appends
        /// remaining full chunks directly to `output` without copying.
        fn drain_side(
            &self,
            head: &mut MC,
            pos: &mut usize,
            list: &mut std::vec::IntoIter<MC>,
            result: &mut MC,
            output: &mut Vec<MC>,
            stash: &mut Vec<MC>,
        ) {
            // Copy the partially-consumed head into result.
            if *pos < head.len() {
                result.merge_from(std::slice::from_mut(head), std::slice::from_mut(pos));
            }
            // Flush result before appending full chunks.
            if !result.is_empty() {
                output.push(std::mem::take(result));
                *result = self.empty(stash);
            }
            // Remaining full chunks go directly to output.
            output.extend(list);
        }
    }

    impl<MC> Merger for InternalMerger<MC>
    where
        MC: InternalMerge<TimeOwned: Ord + PartialOrder + Clone + 'static> + 'static,
    {
        type Time = MC::TimeOwned;
        type Chunk = MC;

        fn merge(
            &mut self,
            list1: Vec<MC>,
            list2: Vec<MC>,
            output: &mut Vec<MC>,
            stash: &mut Vec<MC>,
        ) {
            let mut list1 = list1.into_iter();
            let mut list2 = list2.into_iter();

            let mut heads = [
                list1.next().unwrap_or_default(),
                list2.next().unwrap_or_default(),
            ];
            let mut positions = [0usize, 0usize];

            let mut result = self.empty(stash);

            // Main merge loop: both sides have data.
            while positions[0] < heads[0].len() && positions[1] < heads[1].len() {
                result.merge_from(&mut heads, &mut positions);

                if positions[0] >= heads[0].len() {
                    let old = std::mem::replace(&mut heads[0], list1.next().unwrap_or_default());
                    self.recycle(old, stash);
                    positions[0] = 0;
                }
                if positions[1] >= heads[1].len() {
                    let old = std::mem::replace(&mut heads[1], list2.next().unwrap_or_default());
                    self.recycle(old, stash);
                    positions[1] = 0;
                }
                if result.at_capacity() {
                    output.push(std::mem::take(&mut result));
                    result = self.empty(stash);
                }
            }

            // Drain remaining from each side: copy partial head, then append full chunks.
            self.drain_side(
                &mut heads[0],
                &mut positions[0],
                &mut list1,
                &mut result,
                output,
                stash,
            );
            self.drain_side(
                &mut heads[1],
                &mut positions[1],
                &mut list2,
                &mut result,
                output,
                stash,
            );
            if !result.is_empty() {
                output.push(result);
            }
        }

        fn extract(
            &mut self,
            merged: Vec<Self::Chunk>,
            upper: AntichainRef<Self::Time>,
            frontier: &mut Antichain<Self::Time>,
            ship: &mut Vec<Self::Chunk>,
            kept: &mut Vec<Self::Chunk>,
            stash: &mut Vec<Self::Chunk>,
        ) {
            let mut keep = self.empty(stash);
            let mut ready = self.empty(stash);

            for mut buffer in merged {
                let mut position = 0;
                let len = buffer.len();
                while position < len {
                    buffer.extract(&mut position, upper, frontier, &mut keep, &mut ready);
                    if keep.at_capacity() {
                        kept.push(std::mem::take(&mut keep));
                        keep = self.empty(stash);
                    }
                    if ready.at_capacity() {
                        ship.push(std::mem::take(&mut ready));
                        ready = self.empty(stash);
                    }
                }
                self.recycle(buffer, stash);
            }
            if !keep.is_empty() {
                kept.push(keep);
            }
            if !ready.is_empty() {
                ship.push(ready);
            }
        }

        fn account(chunk: &Self::Chunk) -> (usize, usize, usize, usize) {
            chunk.account()
        }

        fn time_range(
            chunk: &Self::Chunk,
        ) -> Option<(Antichain<Self::Time>, Antichain<Self::Time>)> {
            chunk.time_range()
        }
    }

    /// Implementation of `InternalMerge` for `Vec<(D, T, R)>`.
    ///
    /// Note: The `VecMerger` type implements `Merger` directly and avoids
    /// cloning by draining inputs. This `InternalMerge` impl is retained
    /// because `reduce` requires `Builder::Input: InternalMerge`.
    pub mod vec_internal {
        use super::InternalMerge;
        use differential_dataflow::difference::Semigroup;
        use std::cmp::Ordering;
        use timely::PartialOrder;
        use timely::container::SizableContainer;
        use timely::progress::frontier::{Antichain, AntichainRef};

        impl<
            D: Ord + Clone + 'static,
            T: Ord + Clone + PartialOrder + 'static,
            R: Semigroup + Clone + 'static,
        > InternalMerge for Vec<(D, T, R)>
        {
            type TimeOwned = T;

            fn len(&self) -> usize {
                Vec::len(self)
            }
            fn clear(&mut self) {
                Vec::clear(self)
            }

            fn merge_from(&mut self, others: &mut [Self], positions: &mut [usize]) {
                match others.len() {
                    0 => {}
                    1 => {
                        let other = &mut others[0];
                        let pos = &mut positions[0];
                        if self.is_empty() && *pos == 0 {
                            std::mem::swap(self, other);
                            return;
                        }
                        self.extend_from_slice(&other[*pos..]);
                        *pos = other.len();
                    }
                    2 => {
                        let (left, right) = others.split_at_mut(1);
                        let other1 = &mut left[0];
                        let other2 = &mut right[0];

                        while positions[0] < other1.len()
                            && positions[1] < other2.len()
                            && !self.at_capacity()
                        {
                            let (d1, t1, _) = &other1[positions[0]];
                            let (d2, t2, _) = &other2[positions[1]];
                            // NOTE: The .clone() calls here are not great, but this dead code to be removed in the next release.
                            match (d1, t1).cmp(&(d2, t2)) {
                                Ordering::Less => {
                                    self.push(other1[positions[0]].clone());
                                    positions[0] += 1;
                                }
                                Ordering::Greater => {
                                    self.push(other2[positions[1]].clone());
                                    positions[1] += 1;
                                }
                                Ordering::Equal => {
                                    let (d, t, mut r1) = other1[positions[0]].clone();
                                    let (_, _, ref r2) = other2[positions[1]];
                                    r1.plus_equals(r2);
                                    if !r1.is_zero() {
                                        self.push((d, t, r1));
                                    }
                                    positions[0] += 1;
                                    positions[1] += 1;
                                }
                            }
                        }
                    }
                    n => unimplemented!("{n}-way merge not yet supported"),
                }
            }

            fn extract(
                &mut self,
                position: &mut usize,
                upper: AntichainRef<T>,
                frontier: &mut Antichain<T>,
                keep: &mut Self,
                ship: &mut Self,
            ) {
                let len = self.len();
                while *position < len && !keep.at_capacity() && !ship.at_capacity() {
                    let (data, time, diff) = self[*position].clone();
                    if upper.less_equal(&time) {
                        frontier.insert_with(&time, |time| time.clone());
                        keep.push((data, time, diff));
                    } else {
                        ship.push((data, time, diff));
                    }
                    *position += 1;
                }
            }

            fn time_range(&self) -> Option<(Antichain<T>, Antichain<T>)> {
                if self.is_empty() {
                    return None;
                }
                Some(crate::merge_batcher::build_time_bounds(
                    self.iter().map(|(_, t, _)| t.clone()),
                ))
            }
        }
    }
}
