//! A `Batcher` implementation based on merge sort.
//!
//! The `MergeBatcher` requires a "merger" that implements the [`Merger`] trait, which provides
//! hooks for manipulating sorted "chains" of chunks as needed by the merge batcher: merging
//! chunks and also splitting them apart based on time.
//!
//! Callers feed already-chunked, sorted-and-consolidated input into the batcher via [`PushInto`].
//! Forming such chunks from raw data is the responsibility of the caller (typically a chunker
//! living in the surrounding dataflow operator).

use timely::progress::frontier::AntichainRef;
use timely::progress::{frontier::Antichain, Timestamp};
use timely::container::PushInto;

use crate::logging::{BatcherEvent, Logger};
use crate::trace::{Batcher, Description};

/// Creates batches from chunks of sorted, consolidated tuples.
pub struct MergeBatcher<M: Merger> {
    /// Sorted, consolidated chains, each paired with its cached summed update count.
    ///
    /// The cached count is the chain's *merge weight*: the geometric ladder weighs
    /// chains by updates, not chunk counts, since regrading decouples the two. A
    /// chain is immutable until merged, so the weight is computed once at push.
    ///
    /// Do not push/pop directly but use the corresponding functions ([`Self::chain_push`]/[`Self::chain_pop`]).
    chains: Vec<(usize, Vec<M::Chunk>)>,
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
}

impl<M> Batcher for MergeBatcher<M>
where
    M: Merger<Time: Timestamp>,
{
    type Time = M::Time;
    type Output = M::Chunk;

    fn new(logger: Option<Logger>, operator_id: usize) -> Self {
        Self {
            logger,
            operator_id,
            merger: M::default(),
            chains: Vec::new(),
            stash: Vec::new(),
            frontier: Antichain::new(),
            lower: Antichain::from_elem(M::Time::minimum()),
        }
    }

    // Sealing a batch means finding those updates with times not greater or equal to any time
    // in `upper`. All updates must have time greater or equal to the previously used `upper`,
    // which we call `lower`, by assumption that after sealing a batcher we receive no more
    // updates with times not greater or equal to `upper`.
    fn seal(&mut self, upper: Antichain<M::Time>) -> (Vec<Self::Output>, Description<M::Time>) {
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

        self.merger.extract(merged, upper.borrow(), &mut self.frontier, &mut readied, &mut kept, &mut self.stash);

        if !kept.is_empty() {
            self.chain_push(kept);
        }

        self.stash.clear();

        let description = Description::new(self.lower.clone(), upper.clone(), Antichain::from_elem(M::Time::minimum()));
        self.lower = upper;
        (readied, description)
    }

    /// The frontier of elements remaining after the most recent call to `self.seal`.
    #[inline]
    fn frontier(&mut self) -> AntichainRef<'_, M::Time> {
        self.frontier.borrow()
    }
}

impl<M: Merger> PushInto<M::Chunk> for MergeBatcher<M> {
    fn push_into(&mut self, chunk: M::Chunk) {
        self.insert_chain(vec![chunk]);
    }
}

impl<M: Merger> MergeBatcher<M> {
    /// Insert a chain and maintain chain properties: Chains are geometrically sized
    /// (by summed updates) and ordered by decreasing update weight.
    fn insert_chain(&mut self, chain: Vec<M::Chunk>) {
        if !chain.is_empty() {
            self.chain_push(chain);
            while self.chains.len() > 1 && (self.chains[self.chains.len() - 1].0 >= self.chains[self.chains.len() - 2].0 / 2) {
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
        self.merger.merge(list1, list2, &mut output, &mut self.stash);

        output
    }

    /// Pop a chain and account size changes.
    #[inline]
    fn chain_pop(&mut self) -> Option<Vec<M::Chunk>> {
        let (_weight, chain) = self.chains.pop()?;
        self.account(chain.iter().map(Self::record), -1);
        Some(chain)
    }

    /// Push a chain and account size changes.
    ///
    /// Caches the chain's summed update count alongside it for the ladder.
    #[inline]
    fn chain_push(&mut self, chain: Vec<M::Chunk>) {
        let weight = chain.iter().map(M::len).sum();
        self.account(chain.iter().map(Self::record), 1);
        self.chains.push((weight, chain));
    }

    /// The `(records, size, capacity, allocations)` logger tuple for one chunk,
    /// assembled from the two focused `Merger` methods.
    #[inline]
    fn record(chunk: &M::Chunk) -> (usize, usize, usize, usize) {
        let (size, capacity, allocations) = M::allocation(chunk);
        (M::len(chunk), size, capacity, allocations)
    }

    /// Account size changes. Only performs work if a logger exists.
    ///
    /// Calculate the size based on the iterator passed along, with each attribute
    /// multiplied by `diff`. Usually, one wants to pass 1 or -1 as the diff.
    #[inline]
    fn account<I: IntoIterator<Item = (usize, usize, usize, usize)>>(&self, items: I, diff: isize) {
        if let Some(logger) = &self.logger {
            let (mut records, mut size, mut capacity, mut allocations) = (0isize, 0isize, 0isize, 0isize);
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

impl<M: Merger> Drop for MergeBatcher<M> {
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
    fn merge(&mut self, list1: Vec<Self::Chunk>, list2: Vec<Self::Chunk>, output: &mut Vec<Self::Chunk>, stash: &mut Vec<Self::Chunk>);
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

    /// The number of updates in a chunk.
    ///
    /// Drives the geometric ladder (chains are weighed by summed updates, not chunk
    /// counts, since regrading decouples the two) and the `records` field of the
    /// size logger.
    fn len(chunk: &Self::Chunk) -> usize;

    /// Backing-allocation figures for a chunk: `(size, capacity, allocations)`, for
    /// the size logger's memory telemetry.
    ///
    /// Defaults to zero — most chunk types do not track this. Override to report
    /// real figures (e.g. Materialize's memory accounting).
    fn allocation(_chunk: &Self::Chunk) -> (usize, usize, usize) { (0, 0, 0) }
}

/// A `Merger` implementation for vector update containers.
pub mod vec {

    use std::marker::PhantomData;
    use timely::container::SizableContainer;
    use timely::progress::frontier::{Antichain, AntichainRef};
    use timely::PartialOrder;
    use crate::trace::implementations::merge_batcher::Merger;

    /// A `Merger` implementation for `Vec<(D, T, R)>` that drains owned inputs.
    pub struct VecMerger<D, T, R> {
        _marker: PhantomData<(D, T, R)>,
    }

    impl<D, T, R> Default for VecMerger<D, T, R> {
        fn default() -> Self { Self { _marker: PhantomData } }
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
        fn refill(queue: &mut std::collections::VecDeque<(D, T, R)>, iter: &mut impl Iterator<Item = Vec<(D, T, R)>>, stash: &mut Vec<Vec<(D, T, R)>>) {
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
        R: crate::difference::Semigroup + 'static,
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
            let mut q1 = VecDeque::<(D,T,R)>::from(iter1.next().unwrap_or_default());
            let mut q2 = VecDeque::<(D,T,R)>::from(iter2.next().unwrap_or_default());

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
                if q1.is_empty() { Self::refill(&mut q1, &mut iter1, stash); }
                if q2.is_empty() { Self::refill(&mut q2, &mut iter2, stash); }
            }

            // Push partial result and remaining data from both sides.
            if !result.is_empty() { output.push(result); }
            for q in [q1, q2] {
                if !q.is_empty() { output.push(Vec::from(q)); }
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
            if !keep.is_empty() { kept.push(keep); }
            if !ready.is_empty() { ship.push(ready); }
        }

        fn len(chunk: &Vec<(D, T, R)>) -> usize { chunk.len() }
    }
}

#[cfg(test)]
mod test {
    use timely::progress::frontier::Antichain;
    use crate::trace::Batcher;
    use super::MergeBatcher;
    use super::vec::VecMerger;

    type Bt = MergeBatcher<VecMerger<u64, u64, i64>>;

    /// The sealed frontier must reflect the POST-CONSOLIDATION set of distinct kept times:
    /// two chains carry cancelling updates at a kept time (`t=5`), plus a survivor at a later
    /// kept time (`t=7`). After `seal(upper=[3])` the frontier must be `{7}` — `(100, 5)` nets
    /// to zero and needs no capability. (A per-chain extract that folds the frontier before
    /// consolidating would wrongly report `{5}`.)
    #[test]
    fn frontier_is_post_consolidation() {
        let mut b = Bt::new(None, 0);
        b.chain_push(vec![vec![(100u64, 5u64, 1i64), (200u64, 7u64, 1i64)]]);
        b.chain_push(vec![vec![(100u64, 5u64, -1i64)]]);
        let _ = b.seal(Antichain::from_elem(3));
        let got: Vec<u64> = b.frontier().iter().cloned().collect();
        assert_eq!(got, vec![7u64],
            "frontier held a capability at t=5, which consolidates to zero (got {got:?})");
    }

    /// Sanity: with no cross-chain cancellation, the frontier is the minimal kept time.
    #[test]
    fn frontier_survivor_minimum() {
        let mut b = Bt::new(None, 0);
        b.chain_push(vec![vec![(100u64, 5u64, 1i64)]]);
        b.chain_push(vec![vec![(200u64, 7u64, 1i64)]]);
        let _ = b.seal(Antichain::from_elem(3));
        let got: Vec<u64> = b.frontier().iter().cloned().collect();
        assert_eq!(got, vec![5u64]);
    }
}
