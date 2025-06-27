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

use columnar::{Columnar, Index};
use differential_dataflow::difference::Semigroup;
use differential_dataflow::logging::{BatcherEvent, Logger};
use differential_dataflow::trace::{Batcher, Builder, Description};
use timely::Container;
use timely::container::{ContainerBuilder, PushInto};
use timely::progress::frontier::AntichainRef;
use timely::progress::{Antichain, Timestamp};

use crate::containers::container::PackState;
use crate::containers::{Column, ColumnBuilder, PackedContainer, batcher};

/// Creates batches from containers of unordered tuples.
///
/// To implement `Batcher`, the container builder `C` must accept `&mut Input` as inputs,
/// and must produce outputs of type `M::Chunk`.
pub struct MergeBatcher<Input, D, T, R>
where
    D: for<'a> Columnar<Ref<'a>: Ord>,
    T: for<'a> Columnar<Ref<'a>: Ord> + Timestamp,
    R: Columnar + Default + Semigroup,
{
    /// Transforms input streams to chunks of sorted, consolidated data.
    chunker: batcher::Chunker<ColumnBuilder<(D, T, R)>>,
    /// A sequence of power-of-two length lists of sorted, consolidated containers.
    ///
    /// Do not push/pop directly but use the corresponding functions ([`Self::chain_push`]/[`Self::chain_pop`]).
    chains: Vec<Vec<Column<(D, T, R)>>>,
    /// Stash of empty chunks, recycled through the merging process.
    stash: Vec<Column<(D, T, R)>>,
    /// Current lower frontier, we sealed up to here.
    lower: Antichain<T>,
    /// The lower-bound frontier of the data, after the last call to seal.
    frontier: Antichain<T>,
    /// Logger for size accounting.
    logger: Option<Logger>,
    /// Timely operator ID.
    operator_id: usize,
    /// The `Input` type needs to be called out as the type of container accepted, but it is not otherwise present.
    _marker: PhantomData<Input>,
}

impl<Input, D, T, R> Batcher for MergeBatcher<Input, D, T, R>
where
    D: for<'a> Columnar<Ref<'a>: Ord>,
    T: for<'a> Columnar<Ref<'a>: Ord> + Timestamp,
    R: Columnar + Default + Semigroup,
    batcher::Chunker<ColumnBuilder<(D, T, R)>>: for<'a> PushInto<&'a mut Input>,
{
    type Input = Input;
    type Time = T;
    type Output = Column<(D, T, R)>;

    fn new(logger: Option<Logger>, operator_id: usize) -> Self {
        Self {
            logger,
            operator_id,
            chunker: Default::default(),
            chains: Vec::new(),
            stash: Vec::new(),
            frontier: Antichain::new(),
            lower: Antichain::from_elem(T::minimum()),
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
        upper: Antichain<T>,
    ) -> B::Output {
        // Finish
        while let Some(chunk) = self.chunker.finish() {
            let chunk = std::mem::take(chunk);
            self.insert_chain(vec![chunk]);
        }

        let mut builder = ColumnBuilder::default();
        // Merge all remaining chains into a single chain.
        while self.chains.len() > 1 {
            let list1 = self.chain_pop().unwrap();
            let list2 = self.chain_pop().unwrap();
            let merged = self.merge_by(list1, list2, &mut builder);
            self.chain_push(merged);
        }
        let merged = self.chain_pop().unwrap_or_default();

        // Extract readied data.
        let mut kept = Vec::new();
        let mut readied = Vec::new();
        self.frontier.clear();

        self.extract(merged, upper.borrow(), &mut readied, &mut kept);

        if !kept.is_empty() {
            self.chain_push(kept);
        }

        self.stash.clear();

        let description = Description::new(
            self.lower.clone(),
            upper.clone(),
            Antichain::from_elem(T::minimum()),
        );
        let seal = B::seal(&mut readied, description);
        self.lower = upper;
        seal
    }

    /// The frontier of elements remaining after the most recent call to `self.seal`.
    #[inline(always)]
    fn frontier(&mut self) -> AntichainRef<T> {
        self.frontier.borrow()
    }
}

impl<Input, D, T, R> MergeBatcher<Input, D, T, R>
where
    D: for<'a> Columnar<Ref<'a>: Ord>,
    T: for<'a> Columnar<Ref<'a>: Ord> + Timestamp,
    R: Columnar + Default + Semigroup,
{
    /// Insert a chain and maintain chain properties: Chains are geometrically sized and ordered
    /// by decreasing length.
    fn insert_chain(&mut self, chain: Vec<Column<(D, T, R)>>) {
        if !chain.is_empty() {
            self.chain_push(chain);
            let mut builder = ColumnBuilder::default();
            while self.chains.len() > 1
                && (self.chains[self.chains.len() - 1].len()
                    >= self.chains[self.chains.len() - 2].len() / 2)
            {
                let list1 = self.chain_pop().unwrap();
                let list2 = self.chain_pop().unwrap();
                let merged = self.merge_by(list1, list2, &mut builder);
                self.chain_push(merged);
            }
        }
    }

    // merges two sorted input lists into one sorted output list.
    fn merge_by(
        &mut self,
        list1: Vec<Column<(D, T, R)>>,
        list2: Vec<Column<(D, T, R)>>,
        builder: &mut ColumnBuilder<(D, T, R)>,
    ) -> Vec<Column<(D, T, R)>> {
        // TODO: `list1` and `list2` get dropped; would be better to reuse?
        let mut output = Vec::with_capacity(list1.len() + list2.len());
        self.merge(list1, list2, builder, &mut output);

        output
    }

    /// Pop a chain and account size changes.
    #[inline]
    fn chain_pop(&mut self) -> Option<Vec<Column<(D, T, R)>>> {
        let chain = self.chains.pop();
        self.account(chain.iter().flatten().map(|c| c.account()), -1);
        chain
    }

    /// Push a chain and account size changes.
    #[inline]
    fn chain_push(&mut self, chain: Vec<Column<(D, T, R)>>) {
        self.account(chain.iter().map(|c| c.account()), 1);
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

    /// Helper to extract chunks from the builder and push them into the output.
    #[inline(always)]
    fn extract_chunks(
        &mut self,
        builder: &mut ColumnBuilder<(D, T, R)>,
        output: &mut Vec<Column<(D, T, R)>>,
        state: &mut PackState,
    ) {
        while let Some(chunk) = builder.extract() {
            let mut chunk = std::mem::replace(chunk, self.stash.pop().unwrap_or_default());
            if output.len() > 2 {
                chunk.pack(state);
            }
            output.push(chunk);
        }
    }

    /// Helper to finish the builder and push the chunks into the output.
    #[inline]
    fn finish_chunks(
        &mut self,
        builder: &mut ColumnBuilder<(D, T, R)>,
        output: &mut Vec<Column<(D, T, R)>>,
    ) {
        while let Some(chunk) = builder.finish() {
            let chunk = std::mem::replace(chunk, self.stash.pop().unwrap_or_default());
            output.push_into(chunk);
        }
    }

    fn merge(
        &mut self,
        list1: Vec<Column<(D, T, R)>>,
        list2: Vec<Column<(D, T, R)>>,
        builder: &mut ColumnBuilder<(D, T, R)>,
        output: &mut Vec<Column<(D, T, R)>>,
    ) {
        let mut list1 = list1.into_iter();
        let mut list2 = list2.into_iter();

        let mut state = Default::default();

        let mut head1 = list1.next().unwrap_or_default();
        head1.unpack(&mut state);
        let mut borrow1 = ColumnQueue::new(&head1);
        let mut head2 = list2.next().unwrap_or_default();
        head2.unpack(&mut state);
        let mut borrow2 = ColumnQueue::new(&head2);

        let mut diff_owned = R::default();

        // while we have valid data in each input, merge.
        while !borrow1.is_empty() && !borrow2.is_empty() {
            while !borrow1.is_empty() && !borrow2.is_empty() {
                let cmp = borrow1.cmp_heads(&borrow2);
                // TODO: The following less/greater branches could plausibly be a good moment for
                // `copy_range`, on account of runs of records that might benefit more from a
                // `memcpy`.
                match cmp {
                    Ordering::Less => {
                        builder.push_into(borrow1.pop());
                    }
                    Ordering::Greater => {
                        builder.push_into(borrow2.pop());
                    }
                    Ordering::Equal => {
                        let (data, time, diff1) = borrow1.pop();
                        let (_, _, diff2) = borrow2.pop();

                        diff_owned.copy_from(diff1);
                        let stash2: R = R::into_owned(diff2);
                        diff_owned.plus_equals(&stash2);
                        if !diff_owned.is_zero() {
                            use timely::container::PushInto;
                            builder.push_into((data, time, &diff_owned));
                        }
                    }
                }
            }

            self.extract_chunks(builder, output, &mut state);

            if borrow1.is_empty() {
                let chunk = head1;
                self.stash.push(chunk);
                head1 = list1.next().unwrap_or_default();
                head1.unpack(&mut state);
                borrow1 = ColumnQueue::new(&head1);
            }
            if borrow2.is_empty() {
                let chunk = head2;
                self.stash.push(chunk);
                head2 = list2.next().unwrap_or_default();
                head2.unpack(&mut state);
                borrow2 = ColumnQueue::new(&head2);
            }
        }

        while !borrow1.is_empty() {
            builder.push_into(borrow1.pop());
            self.extract_chunks(builder, output, &mut state);
        }
        self.finish_chunks(builder, output);
        output.extend(list1);

        while !borrow2.is_empty() {
            builder.push_into(borrow2.pop());
            self.extract_chunks(builder, output, &mut state);
        }
        self.finish_chunks(builder, output);
        output.extend(list2);
    }

    fn extract(
        &mut self,
        merged: Vec<Column<(D, T, R)>>,
        upper: AntichainRef<T>,
        readied: &mut Vec<Column<(D, T, R)>>,
        kept: &mut Vec<Column<(D, T, R)>>,
    ) {
        let mut state = Default::default();
        let mut keep = ColumnBuilder::default();
        let mut done = ColumnBuilder::default();

        let mut time_stash = Timestamp::minimum();

        for mut buffer in merged {
            buffer.unpack(&mut state);
            for item in buffer.drain() {
                if Self::time_kept(&item, &upper, &mut self.frontier, &mut time_stash) {
                    keep.push_into(item);
                    self.extract_chunks(&mut keep, kept, &mut state);
                } else {
                    done.push_into(item);
                    self.extract_chunks(&mut done, readied, &mut state);
                }
            }
            // Recycling buffer.
            self.stash.push(buffer);
        }
        // Finish the kept and readied data.
        self.finish_chunks(&mut keep, kept);
        self.finish_chunks(&mut done, readied);
    }

    #[inline]
    fn time_kept(
        (_, time, _): &<(D, T, R) as Columnar>::Ref<'_>,
        upper: &AntichainRef<T>,
        frontier: &mut Antichain<T>,
        stash: &mut T,
    ) -> bool {
        stash.copy_from(*time);
        if upper.less_equal(stash) {
            frontier.insert_ref(stash);
            true
        } else {
            false
        }
    }
}

impl<Input, D, T, R> Drop for MergeBatcher<Input, D, T, R>
where
    D: for<'a> Columnar<Ref<'a>: Ord>,
    T: for<'a> Columnar<Ref<'a>: Ord> + Timestamp,
    R: Columnar + Default + Semigroup,
{
    fn drop(&mut self) {
        // Cleanup chain to retract accounting information.
        while self.chain_pop().is_some() {}
    }
}

/// A queue for extracting items from a `Column` container.
pub struct ColumnQueue<'a, T: Columnar> {
    list: <T::Container as columnar::Container<T>>::Borrowed<'a>,
    head: usize,
}

impl<'a, D, T, R> ColumnQueue<'a, (D, T, R)>
where
    D: for<'b> Columnar<Ref<'b>: Ord>,
    T: for<'b> Columnar<Ref<'b>: Ord>,
    R: Columnar,
{
    #[inline]
    fn pop(&mut self) -> <(D, T, R) as Columnar>::Ref<'a> {
        self.head += 1;
        self.list.get(self.head - 1)
    }
    #[inline]
    fn is_empty(&self) -> bool {
        use columnar::Len;
        self.head == self.list.len()
    }
    #[inline]
    fn cmp_heads<'b>(&self, other: &ColumnQueue<'b, (D, T, R)>) -> Ordering {
        let (data1, time1, _) = self.peek();
        let (data2, time2, _) = other.peek();

        let data1 = D::reborrow(data1);
        let time1 = T::reborrow(time1);
        let data2 = D::reborrow(data2);
        let time2 = T::reborrow(time2);

        (data1, time1).cmp(&(data2, time2))
    }
    #[inline]
    fn new(list: &'a Column<(D, T, R)>) -> ColumnQueue<'a, (D, T, R)> {
        ColumnQueue {
            list: list.borrow(),
            head: 0,
        }
    }
    #[inline]
    fn peek(&self) -> <(D, T, R) as Columnar>::Ref<'a> {
        self.list.get(self.head)
    }
}

impl<D, T, R> Column<(D, T, R)>
where
    D: for<'a> Columnar<Ref<'a>: Ord>,
    T: for<'a> Columnar<Ref<'a>: Ord> + Timestamp,
    for<'a> <T as Columnar>::Ref<'a>: Copy,
    R: Default + Semigroup + Columnar,
{
    fn account(&self) -> (usize, usize, usize, usize) {
        let (mut size, mut cap, mut count) = (0, 0, 0);
        match self {
            Column::Typed(_typed) => {
                // use columnar::HeapSize;
                // let mut cb = |s, c| {
                //     size += s;
                //     cap += c;
                //     count += 1;
                // };
                // data.heap_size(&mut cb);
                // time.heap_size(&mut cb);
                // diff.heap_size(&mut cb);
            }
            Column::Bytes(bytes) => {
                size += bytes.len();
                cap += bytes.len();
                count += 1;
            }
            Column::Align(align) => {
                size += align.len() * 8; // 8 bytes per u64
                cap += align.len() * 8; // 8 bytes per u64
                count += 1;
            }
            Column::Compressed(compressed) => {
                size += compressed.uncompressed_size();
                cap += compressed.capacity();
                count += 1;
            }
        }
        (self.len(), size, cap, count)
    }
}
