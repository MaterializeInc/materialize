// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A general purpose `Batcher` implementation that uses region-allocated chunks of data as its
//! storage.
//!
//! It is almost an exact copy of the columnated merge batcher in Differential, only switching to
//! a different type to store parts of batches.

use differential_dataflow::difference::Semigroup;
use differential_dataflow::logging::{BatcherEvent, DifferentialEvent};
use differential_dataflow::trace::{Batcher, Builder};
use timely::communication::message::RefOrMut;
use timely::container::columnation::Columnation;
use timely::logging::WorkerIdentifier;
use timely::logging_core::Logger;
use timely::progress::{frontier::Antichain, Timestamp};

use crate::containers::fixed_stack::FixedStack;

/// Creates batches from unordered tuples.
pub struct ColumnatedMergeBatcher<K, V, T, D>
where
    K: Columnation + 'static,
    V: Columnation + 'static,
    T: Columnation + 'static,
    D: Columnation + 'static,
{
    sorter: MergeSorterColumnation<(K, V), T, D>,
    lower: Antichain<T>,
    frontier: Antichain<T>,
}

impl<K, V, T, D> Batcher for ColumnatedMergeBatcher<K, V, T, D>
where
    K: Columnation + Ord + Clone + 'static,
    V: Columnation + Ord + Clone + 'static,
    T: Columnation + Timestamp + 'static,
    D: Columnation + Semigroup + 'static,
{
    type Item = ((K, V), T, D);
    type Time = T;

    fn new(
        logger: Option<Logger<DifferentialEvent, WorkerIdentifier>>,
        operator_id: usize,
    ) -> Self {
        ColumnatedMergeBatcher {
            sorter: MergeSorterColumnation::new(logger, operator_id),
            frontier: Antichain::new(),
            lower: Antichain::from_elem(<T as Timestamp>::minimum()),
        }
    }

    #[inline]
    fn push_batch(&mut self, batch: RefOrMut<Vec<Self::Item>>) {
        // `batch` is either a shared reference or an owned allocations.
        match batch {
            RefOrMut::Ref(reference) => {
                // This is a moment at which we could capture the allocations backing
                // `batch` into a different form of region, rather than just  cloning.
                self.sorter.push(&mut reference.clone());
            }
            RefOrMut::Mut(reference) => {
                self.sorter.push(reference);
            }
        }
    }

    // Sealing a batch means finding those updates with times not greater or equal to any time
    // in `upper`. All updates must have time greater or equal to the previously used `upper`,
    // which we call `lower`, by assumption that after sealing a batcher we receive no more
    // updates with times not greater or equal to `upper`.
    #[inline]
    fn seal<B: Builder<Item = Self::Item, Time = Self::Time>>(
        &mut self,
        upper: Antichain<T>,
    ) -> B::Output {
        let mut merged = Default::default();
        self.sorter.finish_into(&mut merged);

        // Determine the number of distinct keys, values, and updates,
        // and form a builder pre-sized for these numbers.
        let mut builder = {
            let mut keys = 0;
            let mut vals = 0;
            let mut upds = 0;
            let mut prev_keyval = None;
            for buffer in merged.iter() {
                for ((key, val), time, _) in buffer.iter() {
                    if !upper.less_equal(time) {
                        if let Some((p_key, p_val)) = prev_keyval {
                            if p_key != key {
                                keys += 1;
                                vals += 1;
                            } else if p_val != val {
                                vals += 1;
                            }
                            upds += 1;
                        } else {
                            keys += 1;
                            vals += 1;
                            upds += 1;
                        }
                        prev_keyval = Some((key, val));
                    }
                }
            }
            B::with_capacity(keys, vals, upds)
        };

        let mut kept = Vec::new();
        let mut keep = FixedStack::default();

        self.frontier.clear();

        for buffer in merged.drain(..) {
            for datum @ ((_key, _val), time, _diff) in &buffer[..] {
                if upper.less_equal(time) {
                    self.frontier.insert(time.clone());
                    if keep.is_empty() {
                        if keep.capacity() == 0 {
                            keep = self.sorter.empty();
                        }
                    } else if keep.len() == keep.capacity() {
                        kept.push(keep);
                        keep = self.sorter.empty();
                    }
                    keep.copy(datum);
                } else {
                    builder.copy(datum);
                }
            }
            // Recycling buffer.
            self.sorter.recycle(buffer);
        }

        // Finish the kept data.
        if !keep.is_empty() {
            kept.push(keep);
        }
        if !kept.is_empty() {
            self.sorter.push_list(kept);
        }

        // Drain buffers (fast reclamation).
        self.sorter.clear_stash();

        let seal = builder.done(
            self.lower.clone(),
            upper.clone(),
            Antichain::from_elem(T::minimum()),
        );
        self.lower = upper;
        seal
    }

    /// The frontier of elements remaining after the most recent call to `self.seal`.
    fn frontier(&mut self) -> timely::progress::frontier::AntichainRef<T> {
        self.frontier.borrow()
    }
}

struct FixedStackQueue<T: Columnation> {
    list: FixedStack<T>,
    head: usize,
}

impl<T: Columnation> Default for FixedStackQueue<T> {
    fn default() -> Self {
        Self::from(Default::default())
    }
}

impl<T: Columnation> FixedStackQueue<T> {
    fn pop(&mut self) -> &T {
        self.head += 1;
        &self.list[self.head - 1]
    }

    fn peek(&self) -> &T {
        &self.list[self.head]
    }

    fn from(list: FixedStack<T>) -> Self {
        FixedStackQueue { list, head: 0 }
    }

    fn done(self) -> FixedStack<T> {
        self.list
    }

    fn is_empty(&self) -> bool {
        self.head == self.list[..].len()
    }

    /// Return an iterator over the remaining elements.
    fn iter(&self) -> impl Iterator<Item = &T> + Clone + ExactSizeIterator {
        self.list[self.head..].iter()
    }
}

struct MergeSorterColumnation<
    D: Columnation + 'static,
    T: Columnation + 'static,
    R: Columnation + 'static,
> {
    /// each power-of-two length list of allocations. Do not push/pop directly but use the corresponding functions.
    queue: Vec<Vec<FixedStack<(D, T, R)>>>,
    stash: Vec<FixedStack<(D, T, R)>>,
    pending: Vec<(D, T, R)>,
    logger: Option<Logger<DifferentialEvent, WorkerIdentifier>>,
    operator_id: usize,
}

impl<
        D: Ord + Columnation + 'static,
        T: Ord + Columnation + 'static,
        R: Semigroup + Columnation + 'static,
    > MergeSorterColumnation<D, T, R>
{
    const BUFFER_SIZE_BYTES: usize = 64 << 10;

    /// Buffer size (number of elements) to use for new/empty buffers.
    const fn buffer_size() -> usize {
        let size = std::mem::size_of::<(D, T, R)>();
        if size == 0 {
            Self::BUFFER_SIZE_BYTES
        } else if size <= Self::BUFFER_SIZE_BYTES {
            Self::BUFFER_SIZE_BYTES / size
        } else {
            1
        }
    }

    fn new(
        logger: Option<Logger<DifferentialEvent, WorkerIdentifier>>,
        operator_id: usize,
    ) -> Self {
        Self {
            logger,
            operator_id,
            queue: Vec::new(),
            stash: Vec::new(),
            pending: Vec::new(),
        }
    }

    fn empty(&mut self) -> FixedStack<(D, T, R)> {
        self.stash
            .pop()
            .unwrap_or_else(|| FixedStack::with_capacity(Self::buffer_size()))
    }

    /// Remove all elements from the stash.
    fn clear_stash(&mut self) {
        self.stash.clear();
    }

    /// Insert an empty buffer into the stash. Panics if the buffer is not empty.
    fn recycle(&mut self, mut buffer: FixedStack<(D, T, R)>) {
        if buffer.capacity() > 0 && self.stash.len() < 2 {
            buffer.clear();
            self.stash.push(buffer);
        }
    }

    fn push(&mut self, batch: &mut Vec<(D, T, R)>) {
        // Ensure `self.pending` has a capacity of `Self::buffer_size`.
        if self.pending.capacity() < Self::buffer_size() {
            self.pending
                .reserve(Self::buffer_size() - self.pending.capacity());
        }

        while !batch.is_empty() {
            self.pending.extend(
                batch.drain(
                    ..std::cmp::min(batch.len(), self.pending.capacity() - self.pending.len()),
                ),
            );
            if self.pending.len() == self.pending.capacity() {
                differential_dataflow::consolidation::consolidate_updates(&mut self.pending);
                if self.pending.len() > self.pending.capacity() / 2 {
                    // Flush if `self.pending` is more than half full after consolidation.
                    self.flush_pending();
                }
            }
        }
    }

    /// Move all elements in `pending` into `queue`. The data in `pending` must be compacted and
    /// sorted. After this function returns, `self.pending` is empty.
    fn flush_pending(&mut self) {
        if !self.pending.is_empty() {
            let mut stack = self.empty();
            stack.reserve_items(self.pending.iter());
            for tuple in self.pending.drain(..) {
                stack.copy(&tuple);
            }
            self.queue_push(vec![stack]);
            while self.queue.len() > 1
                && (self.queue[self.queue.len() - 1].len()
                    >= self.queue[self.queue.len() - 2].len() / 2)
            {
                let list1 = self.queue_pop().unwrap();
                let list2 = self.queue_pop().unwrap();
                let merged = self.merge_by(list1, list2);
                self.queue_push(merged);
            }
        }
    }

    // This is awkward, because it isn't a power-of-two length any more, and we don't want
    // to break it down to be so.
    fn push_list(&mut self, list: Vec<FixedStack<(D, T, R)>>) {
        while self.queue.len() > 1 && self.queue[self.queue.len() - 1].len() < list.len() {
            let list1 = self.queue_pop().unwrap();
            let list2 = self.queue_pop().unwrap();
            let merged = self.merge_by(list1, list2);
            self.queue_push(merged);
        }
        self.queue_push(list);
    }

    fn finish_into(&mut self, target: &mut Vec<FixedStack<(D, T, R)>>) {
        differential_dataflow::consolidation::consolidate_updates(&mut self.pending);
        self.flush_pending();
        while self.queue.len() > 1 {
            let list1 = self.queue_pop().unwrap();
            let list2 = self.queue_pop().unwrap();
            let merged = self.merge_by(list1, list2);
            self.queue_push(merged);
        }

        if let Some(mut last) = self.queue_pop() {
            std::mem::swap(&mut last, target);
        }
    }

    // merges two sorted input lists into one sorted output list.
    fn merge_by(
        &mut self,
        list1: Vec<FixedStack<(D, T, R)>>,
        list2: Vec<FixedStack<(D, T, R)>>,
    ) -> Vec<FixedStack<(D, T, R)>> {
        use std::cmp::Ordering;

        // TODO: `list1` and `list2` get dropped; would be better to reuse?
        let mut output = Vec::with_capacity(list1.len() + list2.len());
        let mut result = self.empty();

        let mut list1 = list1.into_iter();
        let mut list2 = list2.into_iter();

        let mut head1 = FixedStackQueue::from(list1.next().unwrap_or_default());
        let mut head2 = FixedStackQueue::from(list2.next().unwrap_or_default());

        // while we have valid data in each input, merge.
        while !head1.is_empty() && !head2.is_empty() {
            while (result.capacity() - result.len()) > 0 && !head1.is_empty() && !head2.is_empty() {
                let cmp = {
                    let x = head1.peek();
                    let y = head2.peek();
                    (&x.0, &x.1).cmp(&(&y.0, &y.1))
                };
                match cmp {
                    Ordering::Less => {
                        result.copy(head1.pop());
                    }
                    Ordering::Greater => {
                        result.copy(head2.pop());
                    }
                    Ordering::Equal => {
                        let (data1, time1, diff1) = head1.pop();
                        let (_data2, _time2, diff2) = head2.pop();
                        let mut diff1 = diff1.clone();
                        diff1.plus_equals(diff2);
                        if !diff1.is_zero() {
                            result.copy_destructured(data1, time1, &diff1);
                        }
                    }
                }
            }

            if result.capacity() == result.len() {
                output.push(result);
                result = self.empty();
            }

            if head1.is_empty() {
                self.recycle(head1.done());
                head1 = FixedStackQueue::from(list1.next().unwrap_or_default());
            }
            if head2.is_empty() {
                self.recycle(head2.done());
                head2 = FixedStackQueue::from(list2.next().unwrap_or_default());
            }
        }

        if result.len() > 0 {
            output.push(result);
        } else {
            self.recycle(result);
        }

        if !head1.is_empty() {
            let mut result = self.empty();
            result.reserve_items(head1.iter());
            for item in head1.iter() {
                result.copy(item);
            }
            output.push(result);
        }
        output.extend(list1);

        if !head2.is_empty() {
            let mut result = self.empty();
            result.reserve_items(head2.iter());
            for item in head2.iter() {
                result.copy(item);
            }
            output.push(result);
        }
        output.extend(list2);

        output
    }
}

impl<D: Columnation + 'static, T: Columnation + 'static, R: Columnation + 'static>
    MergeSorterColumnation<D, T, R>
{
    /// Pop a batch from `self.queue` and account size changes.
    #[inline]
    fn queue_pop(&mut self) -> Option<Vec<FixedStack<(D, T, R)>>> {
        let batch = self.queue.pop();
        self.account(batch.iter().flatten(), -1);
        batch
    }

    /// Push a batch to `self.queue` and account size changes.
    #[inline]
    fn queue_push(&mut self, batch: Vec<FixedStack<(D, T, R)>>) {
        self.account(&batch, 1);
        self.queue.push(batch);
    }

    /// Account size changes. Only performs work if a logger exists.
    ///
    /// Calculate the size based on the [`FixedStack`]s passed along, with each attribute
    /// multiplied by `diff`. Usually, one wants to pass 1 or -1 as the diff.
    fn account<'a, I: IntoIterator<Item = &'a FixedStack<(D, T, R)>>>(
        &self,
        items: I,
        diff: isize,
    ) {
        if let Some(logger) = &self.logger {
            let (mut records, mut siz, mut capacity, mut allocations) =
                (0isize, 0isize, 0isize, 0isize);
            for stack in items {
                records = records.saturating_add_unsigned(stack.len());
                stack.heap_size(|s, c| {
                    siz = siz.saturating_add_unsigned(s);
                    capacity = capacity.saturating_add_unsigned(c);
                    allocations += isize::from(c > 0);
                });
            }
            logger.log(BatcherEvent {
                operator: self.operator_id,
                records_diff: records * diff,
                size_diff: siz * diff,
                capacity_diff: capacity * diff,
                allocations_diff: allocations * diff,
            })
        }
    }
}

impl<D: Columnation + 'static, T: Columnation + 'static, R: Columnation + 'static> Drop
    for MergeSorterColumnation<D, T, R>
{
    fn drop(&mut self) {
        while self.queue_pop().is_some() {}
    }
}
