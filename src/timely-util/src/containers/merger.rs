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

use std::cmp::Ordering;
use std::marker::PhantomData;

use differential_dataflow::logging::{BatcherEvent, Logger};
use differential_dataflow::trace::implementations::merge_batcher::Merger;
use differential_dataflow::trace::{Batcher, Builder, Description};
use timely::container::{ContainerBuilder, PushInto};
use timely::progress::frontier::AntichainRef;
use timely::progress::{Antichain, Timestamp};
use timely::{Container, Data, PartialOrder};

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
    C: ContainerBuilder<Container = M::Chunk> + Default + for<'a> PushInto<&'a mut Input>,
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
        let mut chain = Vec::new();
        while let Some(chunk) = self.chunker.finish() {
            let chunk = std::mem::take(chunk);
            chain.push(chunk);
        }
        self.insert_chain(chain);
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
    #[inline(always)]
    fn frontier(&mut self) -> AntichainRef<M::Time> {
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

/// An abstraction for a container that can be iterated over, and conclude by returning itself.
pub trait ContainerQueue<'a, C> {
    /// Items exposed by this queue.
    type Item;
    /// The same type as `Self`, but with a different lifetime.
    type SelfGAT<'b>: ContainerQueue<'b, C>;
    /// Returns the next item in the container. Might panic if the container is empty.
    fn pop(&mut self) -> Self::Item;
    /// Indicates whether `pop` will succeed.
    fn is_empty(&mut self) -> bool;
    /// Compare the heads of two queues, where empty queues come last.
    fn cmp_heads(&mut self, other: &mut Self::SelfGAT<'_>) -> Ordering;
    /// Create a new queue from an existing container.
    fn new(container: &'a mut C) -> Self;
}

/// Behavior to observe the time of an item and account for a container's size.
pub trait MergerChunk: Container {
    /// The queue type that can be used to iterate over the container.
    type ContainerQueue<'a>: ContainerQueue<'a, Self>;
    /// An owned time type.
    ///
    /// This type is provided so that users can maintain antichains of something, in order to track
    /// the forward movement of time and extract intervals from chains of updates.
    type TimeOwned: Timestamp;

    /// Relates a borrowed time to antichains of owned times.
    ///
    /// If `upper` is less or equal to `time`, the method returns `true` and ensures that `frontier` reflects `time`.
    fn time_kept(
        time1: &Self::Item<'_>,
        upper: &AntichainRef<Self::TimeOwned>,
        frontier: &mut Antichain<Self::TimeOwned>,
        stash: &mut Self::TimeOwned,
    ) -> bool;

    /// Account the allocations behind the chunk.
    // TODO: Find a more universal home for this: `Container`?
    fn account(&self) -> (usize, usize, usize, usize) {
        let (len, size, capacity, allocations) = (0, 0, 0, 0);
        (len, size, capacity, allocations)
    }
}

/// Push and add two items together, if their summed diff is non-zero.
pub trait PushAndAdd: ContainerBuilder {
    /// The item type that can be pushed into the container.
    type Item<'a>;
    /// The owned diff type.
    ///
    /// This type is provided so that users can provide an owned instance to the `push_and_add` method,
    /// to act as a scratch space when the type is substantial and could otherwise require allocations.
    type DiffOwned: Default;

    /// Push an entry that adds together two diffs.
    ///
    /// This is only called when two items are deemed mergeable by the container queue.
    /// If the two diffs added together is zero do not push anything.
    fn push_and_add(
        &mut self,
        item1: Self::Item<'_>,
        item2: Self::Item<'_>,
        stash: &mut Self::DiffOwned,
    );

    /// State for packing and unpacking the container.
    type State: Default;

    fn pack(&self, _container: &mut Self::Container, _state: &mut Self::State) {
        // Default implementation does nothing.
    }
    fn unpack(&self, _container: &mut Self::Container, _state: &mut Self::State) {
        // Default implementation does nothing.
    }
}

/// A merger for arbitrary containers.
///
/// `MC` is a [`Container`] that implements [`MergerChunk`].
/// `CQ` is a [`ContainerQueue`] supporting `MC`.
#[derive(Default, Debug)]
pub struct ContainerMerger<MCB> {
    builder: MCB,
}

impl<MCB: PushAndAdd> ContainerMerger<MCB> {
    /// Helper to extract chunks from the builder and push them into the output.
    #[inline(always)]
    fn extract_chunks(
        builder: &mut MCB,
        output: &mut Vec<MCB::Container>,
        stash: &mut Vec<MCB::Container>,
        state: &mut MCB::State,
    ) {
        while let Some(chunk) = builder.extract() {
            let mut chunk = std::mem::replace(chunk, stash.pop().unwrap_or_default());
            if output.len() > 2 {
                builder.pack(&mut chunk, state);
            }
            output.push(chunk);
        }
    }

    /// Helper to finish the builder and push the chunks into the output.
    #[inline]
    fn finish_chunks(
        builder: &mut MCB,
        output: &mut Vec<MCB::Container>,
        stash: &mut Vec<MCB::Container>,
        state: &mut MCB::State,
    ) {
        while let Some(chunk) = builder.finish() {
            let chunk = std::mem::replace(chunk, stash.pop().unwrap_or_default());
            output.push_into(chunk);
        }
    }
}

/// The container queue for a merger chunk builder.
type CQ<'a, MCB> = <<MCB as ContainerBuilder>::Container as MergerChunk>::ContainerQueue<'a>;
/// The container queue's item for a merger chunk builder.
type CQI<'a, MCB> = <CQ<'a, MCB> as ContainerQueue<'a, <MCB as ContainerBuilder>::Container>>::Item;

/// The core of the algorithm, implementing the `Merger` trait for `ContainerMerger`.
///
/// The type bounds look intimidating, but they are not too complex:
/// * `MCB` must be a container builder that can absorb its own container's items,
///   and the items produced by the merger chunk's container queue.
/// * The `MCB::Container` must implement `MergerChunk`, which defines the container queue.
/// * The container queue has a `SelfGAT` type that is a lifetime-dependent version of itself,
///   which allows it to be used in the `cmp_heads` function.
impl<MCB> Merger for ContainerMerger<MCB>
where
    MCB: ContainerBuilder
        + for<'a> PushInto<<MCB::Container as Container>::Item<'a>>
        + for<'a> PushInto<CQI<'a, MCB>>
        + for<'a> PushAndAdd<Item<'a> = CQI<'a, MCB>>,
    for<'a, 'b> MCB::Container: MergerChunk<TimeOwned: Ord + PartialOrder + Data>,
    for<'a, 'b> CQ<'a, MCB>: ContainerQueue<'a, MCB::Container, SelfGAT<'b> = CQ<'b, MCB>>,
{
    type Time = <MCB::Container as MergerChunk>::TimeOwned;
    type Chunk = MCB::Container;

    fn merge(
        &mut self,
        list1: Vec<Self::Chunk>,
        list2: Vec<Self::Chunk>,
        output: &mut Vec<Self::Chunk>,
        stash: &mut Vec<Self::Chunk>,
    ) {
        let mut list1 = list1.into_iter();
        let mut list2 = list2.into_iter();

        let mut state = Default::default();

        let mut head1 = list1.next().unwrap_or_default();
        self.builder.unpack(&mut head1, &mut state);
        let mut borrow1 = CQ::<MCB>::new(&mut head1);
        let mut head2 = list2.next().unwrap_or_default();
        self.builder.unpack(&mut head2, &mut state);
        let mut borrow2 = CQ::<MCB>::new(&mut head2);

        let mut diff_owned = Default::default();

        // while we have valid data in each input, merge.
        while !borrow1.is_empty() && !borrow2.is_empty() {
            while !borrow1.is_empty() && !borrow2.is_empty() {
                let cmp = borrow1.cmp_heads(&mut borrow2);
                // TODO: The following less/greater branches could plausibly be a good moment for
                // `copy_range`, on account of runs of records that might benefit more from a
                // `memcpy`.
                match cmp {
                    Ordering::Less => {
                        self.builder.push_into(borrow1.pop());
                    }
                    Ordering::Greater => {
                        self.builder.push_into(borrow2.pop());
                    }
                    Ordering::Equal => {
                        let item1 = borrow1.pop();
                        let item2 = borrow2.pop();
                        self.builder.push_and_add(item1, item2, &mut diff_owned);
                    }
                }
            }

            Self::extract_chunks(&mut self.builder, output, stash, &mut state);

            if borrow1.is_empty() {
                drop(borrow1);
                let chunk = head1;
                stash.push(chunk);
                head1 = list1.next().unwrap_or_default();
                self.builder.unpack(&mut head1, &mut state);
                borrow1 = CQ::<MCB>::new(&mut head1);
            }
            if borrow2.is_empty() {
                drop(borrow2);
                let chunk = head2;
                stash.push(chunk);
                head2 = list2.next().unwrap_or_default();
                self.builder.unpack(&mut head2, &mut state);
                borrow2 = CQ::<MCB>::new(&mut head2);
            }
        }

        while !borrow1.is_empty() {
            self.builder.push_into(borrow1.pop());
            Self::extract_chunks(&mut self.builder, output, stash, &mut state);
        }
        Self::finish_chunks(&mut self.builder, output, stash, &mut state);
        output.extend(list1);

        while !borrow2.is_empty() {
            self.builder.push_into(borrow2.pop());
            Self::extract_chunks(&mut self.builder, output, stash, &mut state);
        }
        Self::finish_chunks(&mut self.builder, output, stash, &mut state);
        output.extend(list2);
    }

    fn extract(
        &mut self,
        merged: Vec<Self::Chunk>,
        upper: AntichainRef<Self::Time>,
        frontier: &mut Antichain<Self::Time>,
        readied: &mut Vec<Self::Chunk>,
        kept: &mut Vec<Self::Chunk>,
        stash: &mut Vec<Self::Chunk>,
    ) {
        let mut state = Default::default();
        let mut keep = MCB::default();

        let mut time_stash = Self::Time::minimum();

        for mut buffer in merged {
            self.builder.unpack(&mut buffer, &mut state);
            for item in buffer.drain() {
                if <MCB::Container as MergerChunk>::time_kept(
                    &item,
                    &upper,
                    frontier,
                    &mut time_stash,
                ) {
                    keep.push_into(item);
                    Self::extract_chunks(&mut keep, kept, stash, &mut state);
                } else {
                    self.builder.push_into(item);
                    Self::extract_chunks(&mut self.builder, readied, stash, &mut state);
                }
            }
            // Recycling buffer.
            stash.push(buffer);
        }
        // Finish the kept and readied data.
        Self::finish_chunks(&mut keep, kept, stash, &mut state);
        Self::finish_chunks(&mut self.builder, readied, stash, &mut state);
    }

    /// Account the allocations behind the chunk.
    fn account(chunk: &Self::Chunk) -> (usize, usize, usize, usize) {
        chunk.account()
    }
}

/// Implementations of `ContainerQueue` and `MergerChunk` for `Vec` containers.
pub mod vec {
    use differential_dataflow::difference::Semigroup;
    use timely::Data;
    use timely::container::{CapacityContainerBuilder, PushInto};
    use timely::progress::{Antichain, Timestamp, frontier::AntichainRef};

    use crate::containers::merger::{ContainerMerger, ContainerQueue, MergerChunk, PushAndAdd};

    /// A `Merger` implementation backed by vector containers.
    pub type VecMerger<D, T, R> = ContainerMerger<CapacityContainerBuilder<Vec<(D, T, R)>>>;

    impl<D, T, R> PushAndAdd for CapacityContainerBuilder<Vec<(D, T, R)>>
    where
        D: Data,
        T: Data,
        R: Data + Semigroup,
    {
        type Item<'a> = (D, T, R);
        type DiffOwned = ();

        fn push_and_add<'a>(
            &mut self,
            (data, time, mut diff1): (D, T, R),
            (_, _, diff2): (D, T, R),
            _stash: &mut Self::DiffOwned,
        ) {
            diff1.plus_equals(&diff2);
            if !diff1.is_zero() {
                self.push_into((data, time, diff1));
            }
        }

        type State = ();
    }

    impl<'a, D, T, R> ContainerQueue<'a, Vec<(D, T, R)>>
        for std::iter::Peekable<std::vec::Drain<'a, (D, T, R)>>
    where
        D: Ord + 'static,
        T: Ord + 'static,
        R: 'static,
    {
        type Item = (D, T, R);
        type SelfGAT<'b> = std::iter::Peekable<std::vec::Drain<'b, (D, T, R)>>;
        fn pop(&mut self) -> Self::Item {
            self.next()
                .expect("ContainerQueue: pop called on empty queue")
        }
        fn is_empty(&mut self) -> bool {
            self.peek().is_none()
        }
        fn cmp_heads(&mut self, other: &mut Self::SelfGAT<'_>) -> std::cmp::Ordering {
            let (data1, time1, _) = self.peek().unwrap();
            let (data2, time2, _) = other.peek().unwrap();
            (data1, time1).cmp(&(data2, time2))
        }
        fn new(list: &'a mut Vec<(D, T, R)>) -> Self {
            list.drain(..).peekable()
        }
    }

    impl<D: Ord + 'static, T: Timestamp, R: Semigroup + 'static> MergerChunk for Vec<(D, T, R)> {
        type ContainerQueue<'a> = std::iter::Peekable<std::vec::Drain<'a, (D, T, R)>>;
        type TimeOwned = T;

        fn time_kept(
            (_, time, _): &Self::Item<'_>,
            upper: &AntichainRef<Self::TimeOwned>,
            frontier: &mut Antichain<Self::TimeOwned>,
            _stash: &mut T,
        ) -> bool {
            if upper.less_equal(time) {
                frontier.insert_with(time, |time| time.clone());
                true
            } else {
                false
            }
        }
        fn account(&self) -> (usize, usize, usize, usize) {
            let (size, capacity, allocations) = (0, 0, 0);
            (self.len(), size, capacity, allocations)
        }
    }
}

/// Implementations of `ContainerQueue` and `MergerChunk` for `TimelyStack` containers (columnation).
pub mod columnation {
    use std::collections::VecDeque;

    use columnation::Columnation;
    use differential_dataflow::containers::TimelyStack;
    use differential_dataflow::difference::Semigroup;
    use timely::Data;
    use timely::container::{ContainerBuilder, PushInto, SizableContainer};
    use timely::progress::{Antichain, Timestamp, frontier::AntichainRef};

    use crate::containers::merger::{ContainerMerger, ContainerQueue, MergerChunk, PushAndAdd};

    /// A `Merger` implementation backed by `TimelyStack` containers (columnation).
    pub type ColMerger<D, T, R> = ContainerMerger<TimelyStackBuilder<(D, T, R)>>;

    impl<'a, D, T, R> ContainerQueue<'a, TimelyStack<(D, T, R)>>
        for std::iter::Peekable<std::slice::Iter<'a, (D, T, R)>>
    where
        D: Ord + Columnation + 'static,
        T: Ord + Columnation + 'static,
        R: Columnation + 'static,
    {
        type Item = &'a (D, T, R);
        type SelfGAT<'b> = std::iter::Peekable<std::slice::Iter<'b, (D, T, R)>>;

        fn pop(&mut self) -> Self::Item {
            self.next()
                .expect("ContainerQueue: pop called on empty queue")
        }
        fn is_empty(&mut self) -> bool {
            self.peek().is_none()
        }
        fn cmp_heads(&mut self, other: &mut Self::SelfGAT<'_>) -> std::cmp::Ordering {
            let (data1, time1, _) = self.peek().unwrap();
            let (data2, time2, _) = other.peek().unwrap();
            (data1, time1).cmp(&(data2, time2))
        }
        fn new(list: &'a mut TimelyStack<(D, T, R)>) -> Self {
            list.iter().peekable()
        }
    }

    impl<D, T, R> MergerChunk for TimelyStack<(D, T, R)>
    where
        D: Ord + Columnation + 'static,
        T: Timestamp + Columnation,
        R: Default + Semigroup + Columnation + 'static,
    {
        type ContainerQueue<'a> = std::iter::Peekable<std::slice::Iter<'a, (D, T, R)>>;
        type TimeOwned = T;
        fn time_kept(
            (_, time, _): &Self::Item<'_>,
            upper: &AntichainRef<Self::TimeOwned>,
            frontier: &mut Antichain<Self::TimeOwned>,
            _stash: &mut T,
        ) -> bool {
            if upper.less_equal(time) {
                frontier.insert_with(time, |time| time.clone());
                true
            } else {
                false
            }
        }
        fn account(&self) -> (usize, usize, usize, usize) {
            let (mut size, mut capacity, mut allocations) = (0, 0, 0);
            let cb = |siz, cap| {
                size += siz;
                capacity += cap;
                allocations += 1;
            };
            self.heap_size(cb);
            (self.len(), size, capacity, allocations)
        }
    }

    /// A capacity container builder for TimelyStack where we have access to the internals.
    /// We need to have a different `push_into` implementation that can call `copy_destructured`
    /// to avoid unnecessary allocations.
    ///
    /// A container builder that uses length and preferred capacity to chunk data for [`TimelyStack`].
    ///
    /// Maintains a single empty allocation between [`Self::push_into`] and [`Self::extract`], but not
    /// across [`Self::finish`] to maintain a low memory footprint.
    ///
    /// Maintains FIFO order.
    #[derive(Debug)]
    pub struct TimelyStackBuilder<C: Columnation> {
        /// Container that we're writing to.
        pub(crate) current: TimelyStack<C>,
        /// Empty allocation.
        pub(crate) empty: Option<TimelyStack<C>>,
        /// Completed containers pending to be sent.
        pub(crate) pending: VecDeque<TimelyStack<C>>,
    }

    impl<C: Columnation> Default for TimelyStackBuilder<C> {
        fn default() -> Self {
            Self {
                current: TimelyStack::default(),
                empty: None,
                pending: VecDeque::new(),
            }
        }
    }

    impl<T: Columnation> PushInto<&T> for TimelyStackBuilder<T> {
        #[inline(always)]
        fn push_into(&mut self, item: &T) {
            // Ensure capacity
            self.current.ensure_capacity(&mut self.empty);

            // Push item
            self.current.copy(item);

            // Maybe flush
            if self.current.at_capacity() {
                self.pending.push_back(std::mem::take(&mut self.current));
            }
        }
    }

    impl<C: Columnation + Clone + 'static> ContainerBuilder for TimelyStackBuilder<C> {
        type Container = TimelyStack<C>;

        #[inline]
        fn extract(&mut self) -> Option<&mut TimelyStack<C>> {
            if let Some(container) = self.pending.pop_front() {
                self.empty = Some(container);
                self.empty.as_mut()
            } else {
                None
            }
        }

        #[inline]
        fn finish(&mut self) -> Option<&mut TimelyStack<C>> {
            if !self.current.is_empty() {
                self.pending.push_back(std::mem::take(&mut self.current));
            }
            self.empty = self.pending.pop_front();
            self.empty.as_mut()
        }
    }

    impl<D, T, R> PushAndAdd for TimelyStackBuilder<(D, T, R)>
    where
        D: Data + Columnation,
        T: Data + Columnation,
        R: Data + Columnation + Default + Semigroup,
    {
        type Item<'a> = &'a (D, T, R);
        type DiffOwned = R;

        fn push_and_add(
            &mut self,
            (data, time, diff1): &(D, T, R),
            (_, _, diff2): &(D, T, R),
            stash: &mut Self::DiffOwned,
        ) {
            stash.clone_from(diff1);
            stash.plus_equals(diff2);
            if !stash.is_zero() {
                // Ensure capacity
                self.current.ensure_capacity(&mut self.empty);

                // Push item
                self.current.copy_destructured(data, time, stash);

                // Maybe flush
                if self.current.at_capacity() {
                    self.pending.push_back(std::mem::take(&mut self.current));
                }
            }
        }

        type State = ();
    }
}
