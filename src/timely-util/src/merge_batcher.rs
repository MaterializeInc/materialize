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
}

pub use container::InternalMerger;

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
        }
    }
}
