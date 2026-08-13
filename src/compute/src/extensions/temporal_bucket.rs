// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utilities and stream extensions for temporal bucketing.

use columnar::{Columnar, Index, Len};
use differential_dataflow::Hashable;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Batcher;
use differential_dataflow::trace::chunk::ChunkBatcher;
use mz_timely_util::columnar::Column;
use mz_timely_util::columnar::batcher::ColumnChunker;
use mz_timely_util::columnar::chunk::ColumnChunk;
use mz_timely_util::temporal::{Bucket, BucketChain, BucketTimestamp};
use timely::Accountable;
use timely::container::PushInto;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Operator;
use timely::dataflow::{Stream, StreamVec};
use timely::order::TotalOrder;
use timely::progress::{Antichain, PathSummary, Timestamp};
use timely::{ContainerBuilder, ExchangeData};

use crate::typedefs::MzData;

/// Sort outstanding updates into a [`BucketChain`], and reveal data not in advance of the input
/// frontier. Retains a capability at the last input frontier to retain the right to produce data
/// at times between the last input frontier and the current input frontier.
pub trait TemporalBucketing<'scope, T: Timestamp, O> {
    /// Construct a new stream that stores updates into a [`BucketChain`] and reveals data
    /// not in advance of the frontier. Data that is within `threshold` distance of the input
    /// frontier or the `as_of` is passed through without being stored in the chain.
    fn bucket<CB>(
        self,
        as_of: Antichain<T>,
        threshold: T::Summary,
    ) -> Stream<'scope, T, CB::Container>
    where
        CB: ContainerBuilder + PushInto<O>;
}

/// Implementation for streams in scopes where timestamps define a total order.
impl<'scope, T, D> TemporalBucketing<'scope, T, (D, T, mz_repr::Diff)>
    for StreamVec<'scope, T, (D, T, mz_repr::Diff)>
where
    T: Timestamp + Default + ExchangeData + MzData + BucketTimestamp + TotalOrder + Lattice,
    D: ExchangeData + MzData + Ord + Clone + std::fmt::Debug + Hashable,
{
    fn bucket<CB>(
        self,
        as_of: Antichain<T>,
        threshold: T::Summary,
    ) -> Stream<'scope, T, CB::Container>
    where
        CB: ContainerBuilder + PushInto<(D, T, mz_repr::Diff)>,
    {
        let scope = self.scope();
        let logger = scope
            .worker()
            .logger_for("differential/arrange")
            .map(Into::into);

        let pact = Exchange::new(|(d, _, _): &(D, T, mz_repr::Diff)| d.hashed().into());
        self.unary_frontier::<CB, _, _, _>(pact, "Temporal delay", |cap, info| {
            let mut chain = BucketChain::new(MergeBatcherWrapper::new(logger, info.global_id));
            let activator = scope.activator_for(info.address);

            // Cap tracking the lower bound of potentially outstanding data.
            let mut cap = Some(cap);

            // Buffer for data to be inserted into the chain.
            let mut buffer = Vec::new();

            move |(input, frontier), output| {
                // The upper frontier is the join of the input frontier and the `as_of` frontier,
                // with the `threshold` summary applied to it.
                let mut upper = Antichain::new();
                for time1 in &frontier.frontier() {
                    for time2 in as_of.elements() {
                        // TODO: Use `join_assign` if we ever use a timestamp with allocations.
                        if let Some(time) = threshold.results_in(&time1.join(time2)) {
                            upper.insert(time);
                        }
                    }
                }

                input.for_each_time(|time, data| {
                    let mut session = output.session_with_builder(&time);
                    for data in data {
                        // Skip data that is about to be revealed.
                        let pass_through = data.extract_if(.., |(_, t, _)| !upper.less_equal(t));
                        session.give_iterator(pass_through);

                        // Sort data by time, then drain it into a buffer that contains data for a
                        // single bucket. We scan the data for ranges of time that fall into the same
                        // bucket so we can push batches of data at once.
                        data.sort_unstable_by(|(_, t, _), (_, t2, _)| t.cmp(t2));

                        let mut drain = data.drain(..);
                        if let Some((datum, time, diff)) = drain.next() {
                            let mut range = chain.range_of(&time).expect("Must exist");
                            buffer.push((datum, time, diff));
                            for (datum, time, diff) in drain {
                                // If we have a range, check if the time is not within it.
                                if !range.contains(&time) {
                                    // If the time is outside the range, push the current buffer
                                    // to the chain and reset the range.
                                    if !buffer.is_empty() {
                                        let bucket =
                                            chain.find_mut(&range.start).expect("Must exist");
                                        bucket.push_container(&mut buffer);
                                        buffer.clear();
                                    }
                                    range = chain.range_of(&time).expect("Must exist");
                                }
                                buffer.push((datum, time, diff));
                            }

                            // Handle leftover data in the buffer.
                            if !buffer.is_empty() {
                                let bucket = chain.find_mut(&range.start).expect("Must exist");
                                bucket.push_container(&mut buffer);
                                buffer.clear();
                            }
                        }
                    }
                });

                // Check for data that is ready to be revealed.
                let peeled = chain.peel(upper.borrow());
                if let Some(cap) = cap.as_ref() {
                    let mut session = output.session_with_builder(cap);
                    for chunk in peeled.into_iter().flat_map(|x| x.done()) {
                        // The chunk batcher hands back `ColumnChunk`s; the output builder
                        // consumes owned `(D, T, Diff)` tuples, so load each chunk's body
                        // and reconstitute each record from its columnar reference.
                        let column = chunk.into_column();
                        session.give_iterator(
                            column
                                .borrow()
                                .into_index_iter()
                                .map(<(D, T, mz_repr::Diff)>::into_owned),
                        );
                    }
                } else {
                    // If we don't have a cap, we should not have any data to reveal.
                    assert!(
                        peeled
                            .into_iter()
                            .flat_map(|x| x.done())
                            .all(|chunk| chunk.record_count() == 0),
                        "Unexpected data revealed without a cap."
                    );
                }

                // Downgrade the cap to the current input frontier.
                if frontier.is_empty() || upper.is_empty() {
                    cap = None;
                } else if let Some(cap) = cap.as_mut() {
                    // TODO: This assumes that the time is total ordered.
                    cap.downgrade(&upper[0]);
                }

                // Maintain the bucket chain by restoring it with fuel.
                let mut fuel = 1_000_000;
                chain.restore(&mut fuel);
                if fuel <= 0 {
                    // If we run out of fuel, we activate the operator to continue processing.
                    activator.activate();
                }
            }
        })
    }
}

/// A wrapper around [`ChunkBatcher`] that implements the bucketing API.
///
/// This is the same chunk merge batcher the default arrangement uses, so the
/// bucket chain and arrangements share a single merge-batcher implementation.
/// The batcher consumes sorted, consolidated [`ColumnChunk`] input, so this
/// wrapper carries a [`ColumnChunker`] that sorts and consolidates the `Vec`
/// input into the [`Column`] chunks it wraps for the batcher.
struct MergeBatcherWrapper<D, T, R>
where
    D: MzData,
    T: MzData + Ord + Default + Timestamp + Lattice,
    R: MzData + Semigroup + Default + for<'a> Semigroup<columnar::Ref<'a, R>>,
    for<'a> columnar::Ref<'a, R>: Ord,
{
    logger: Option<differential_dataflow::logging::Logger>,
    operator_id: usize,
    chunker: ColumnChunker<(D, T, R)>,
    inner: ChunkBatcher<ColumnChunk<D, T, R>>,
}

impl<D, T, R> MergeBatcherWrapper<D, T, R>
where
    D: MzData + Ord + Clone,
    T: MzData + Ord + Clone + Default + Timestamp + Lattice,
    R: MzData + Semigroup + Default + for<'a> Semigroup<columnar::Ref<'a, R>>,
    for<'a> columnar::Ref<'a, R>: Ord,
    for<'a> <D as Columnar>::Container: columnar::Push<columnar::Ref<'a, D>>,
    for<'a> <T as Columnar>::Container: columnar::Push<columnar::Ref<'a, T>>,
    for<'a> <R as Columnar>::Container: columnar::Push<&'a R>,
    for<'a> <(D, T, R) as Columnar>::Container: columnar::Push<&'a (D, T, R)>,
{
    /// Construct a new `MergeBatcherWrapper` with the given logger and operator ID.
    fn new(logger: Option<differential_dataflow::logging::Logger>, operator_id: usize) -> Self {
        Self {
            logger: logger.clone(),
            operator_id,
            chunker: ColumnChunker::default(),
            inner: ChunkBatcher::new(logger, operator_id),
        }
    }

    /// Consolidate `buffer` through the chunker and feed any complete chunks to
    /// the batcher. Empties `buffer`, retaining its capacity.
    fn push_container(&mut self, buffer: &mut Vec<(D, T, R)>) {
        use timely::container::{ContainerBuilder as _, PushInto as _};
        if buffer.is_empty() {
            return;
        }
        // The chunker consumes `Column` input, so stage the `Vec` updates into a
        // raw `Column` first; the chunker then sorts and consolidates them.
        let mut raw: Column<(D, T, R)> = Default::default();
        for update in buffer.drain(..) {
            raw.push_into(&update);
        }
        self.chunker.push_into(&mut raw);
        while let Some(chunk) = self.chunker.extract() {
            let column = std::mem::take(chunk);
            if column.record_count() > 0 {
                self.inner.push_into(ColumnChunk::from_column(column));
            }
        }
    }

    /// Flush any partial chunk still held by the chunker into the batcher.
    fn flush(&mut self) {
        use timely::container::{ContainerBuilder as _, PushInto as _};
        while let Some(chunk) = self.chunker.finish() {
            let column = std::mem::take(chunk);
            if column.record_count() > 0 {
                self.inner.push_into(ColumnChunk::from_column(column));
            }
        }
    }

    /// Reveal the contents of the merge batcher, returning a vector of chunks.
    fn done(mut self) -> Vec<ColumnChunk<D, T, R>> {
        self.flush();
        let (chain, _description) = self.inner.seal(Antichain::new());
        chain
    }
}

impl<D, T, R> Bucket for MergeBatcherWrapper<D, T, R>
where
    D: MzData + Ord + Clone,
    T: MzData + Ord + Clone + Default + Lattice + BucketTimestamp,
    R: MzData + Semigroup + Default + for<'a> Semigroup<columnar::Ref<'a, R>>,
    for<'a> columnar::Ref<'a, R>: Ord,
    for<'a> <D as Columnar>::Container: columnar::Push<columnar::Ref<'a, D>>,
    for<'a> <T as Columnar>::Container: columnar::Push<columnar::Ref<'a, T>>,
    for<'a> <R as Columnar>::Container: columnar::Push<&'a R>,
    for<'a> <(D, T, R) as Columnar>::Container: columnar::Push<&'a (D, T, R)>,
{
    type Timestamp = T;

    fn split(mut self, timestamp: &Self::Timestamp, fuel: &mut i64) -> (Self, Self) {
        use timely::container::PushInto as _;
        self.flush();
        let upper = Antichain::from_elem(timestamp.clone());
        let mut lower = Self::new(self.logger.clone(), self.operator_id);
        // Sealing at `timestamp` ships exactly the updates strictly less than it,
        // as sorted, consolidated chunks; feed them to the lower batcher whole,
        // so spilled bodies move without being loaded.
        let (chain, _description) = self.inner.seal(upper);
        for chunk in chain {
            *fuel = fuel.saturating_sub(chunk.record_count());
            lower.inner.push_into(chunk);
        }
        (lower, self)
    }
}
