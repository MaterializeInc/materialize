// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utilities and stream extensions for temporal bucketing.

use std::marker::PhantomData;

use differential_dataflow::Hashable;
use differential_dataflow::containers::TimelyStack;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::implementations::chunker::ColumnationChunker;
use differential_dataflow::trace::implementations::merge_batcher::{ColMerger, MergeBatcher};
use differential_dataflow::trace::{Batcher, Builder, Description};
use mz_timely_util::temporal::{Bucket, BucketChain, BucketTimestamp};
use timely::container::{ContainerBuilder, PushInto};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Operator;
use timely::dataflow::{Scope, StreamCore};
use timely::order::TotalOrder;
use timely::progress::{Antichain, PathSummary, Timestamp};
use timely::{Data, ExchangeData, PartialOrder};

use crate::typedefs::MzData;

/// Sort outstanding updates into a [`BucketChain`], and reveal data not in advance of the input
/// frontier. Retains a capability at the last input frontier to retain the right to produce data
/// at times between the last input frontier and the current input frontier.
pub trait TemporalBucketing<G: Scope, O> {
    /// Construct a new stream that stores updates into a [`BucketChain`] and reveals data
    /// not in advance of the frontier. Data that is within `threshold` distance of the input
    /// frontier or the `as_of` is passed through without being stored in the chain.
    fn bucket<CB>(
        &self,
        as_of: Antichain<G::Timestamp>,
        threshold: <G::Timestamp as Timestamp>::Summary,
    ) -> StreamCore<G, CB::Container>
    where
        CB: ContainerBuilder + PushInto<O>;
}

/// Implementation for streams in scopes where timestamps define a total order.
impl<G, D> TemporalBucketing<G, (D, G::Timestamp, mz_repr::Diff)>
    for StreamCore<G, Vec<(D, G::Timestamp, mz_repr::Diff)>>
where
    G: Scope<Timestamp: ExchangeData + MzData + BucketTimestamp + TotalOrder + Lattice>,
    D: ExchangeData + MzData + Ord + std::fmt::Debug + Hashable,
{
    fn bucket<CB>(
        &self,
        as_of: Antichain<G::Timestamp>,
        threshold: <G::Timestamp as Timestamp>::Summary,
    ) -> StreamCore<G, CB::Container>
    where
        CB: ContainerBuilder + PushInto<(D, G::Timestamp, mz_repr::Diff)>,
    {
        let scope = self.scope();
        let logger = scope.logger_for("differential/arrange").map(Into::into);

        let pact = Exchange::new(|(d, _, _): &(D, G::Timestamp, mz_repr::Diff)| d.hashed().into());
        self.unary_frontier::<CB, _, _, _>(pact, "Temporal delay", |cap, info| {
            let mut chain = BucketChain::new(MergeBatcherWrapper::new(logger, info.global_id));
            let activator = scope.activator_for(info.address);

            // Cap tracking the lower bound of potentially outstanding data.
            let mut cap = Some(cap);

            // Buffer for data to be inserted into the chain.
            let mut buffer = Vec::new();

            move |input, output| {
                // The upper frontier is the join of the input frontier and the `as_of` frontier,
                // with the `threshold` summary applied to it.
                let mut upper = Antichain::new();
                for time1 in &input.frontier().frontier() {
                    for time2 in as_of.elements() {
                        // TODO: Use `join_assign` if we ever use a timestamp with allocations.
                        if let Some(time) = threshold.results_in(&time1.join(time2)) {
                            upper.insert(time);
                        }
                    }
                }

                while let Some((time, data)) = input.next() {
                    // Skip data that is about to be revealed.
                    let pass_through = data.extract_if(.., |(_, t, _)| !upper.less_equal(t));
                    output
                        .session_with_builder(&time)
                        .give_iterator(pass_through);

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
                                    let bucket = chain.find_mut(&range.start).expect("Must exist");
                                    bucket.inner.push_container(&mut buffer);
                                    buffer.clear();
                                }
                                range = chain.range_of(&time).expect("Must exist");
                            }
                            buffer.push((datum, time, diff));
                        }

                        // Handle leftover data in the buffer.
                        if !buffer.is_empty() {
                            let bucket = chain.find_mut(&range.start).expect("Must exist");
                            bucket.inner.push_container(&mut buffer);
                            buffer.clear();
                        }
                    }
                }

                // Check for data that is ready to be revealed.
                let peeled = chain.peel(upper.borrow());
                if let Some(cap) = cap.as_ref() {
                    let mut session = output.session_with_builder(cap);
                    for stack in peeled.into_iter().flat_map(|x| x.done()) {
                        // TODO: If we have a columnar merge batcher, cloning won't be necessary.
                        session.give_iterator(stack.iter().cloned());
                    }
                } else {
                    // If we don't have a cap, we should not have any data to reveal.
                    assert_eq!(
                        peeled.into_iter().flat_map(|x| x.done()).next(),
                        None,
                        "Unexpected data revealed without a cap."
                    );
                }

                // Downgrade the cap to the current input frontier.
                if input.frontier().is_empty() || upper.is_empty() {
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

/// A wrapper around `MergeBatcher` that implements the `Storage` trait for bucketing.
struct MergeBatcherWrapper<D, T, R>
where
    D: MzData + Ord,
    T: MzData + Ord + PartialOrder + Clone,
    R: MzData + Semigroup + Default,
{
    logger: Option<differential_dataflow::logging::Logger>,
    operator_id: usize,
    inner: MergeBatcher<Vec<(D, T, R)>, ColumnationChunker<(D, T, R)>, ColMerger<D, T, R>>,
}

impl<D, T, R> MergeBatcherWrapper<D, T, R>
where
    D: MzData + Ord + Clone,
    T: MzData + Ord + PartialOrder + Clone + Timestamp,
    R: MzData + Semigroup + Default,
{
    /// Construct a new `MergeBatcherWrapper` with the given logger and operator ID.
    fn new(logger: Option<differential_dataflow::logging::Logger>, operator_id: usize) -> Self {
        Self {
            logger: logger.clone(),
            operator_id,
            inner: MergeBatcher::new(logger, operator_id),
        }
    }

    /// Reveal the contents of the `MergeBatcher`, returning a vector of `TimelyStack`s.
    fn done(mut self) -> Vec<TimelyStack<(D, T, R)>> {
        self.inner.seal::<CapturingBuilder<_, _>>(Antichain::new())
    }
}

impl<D, T, R> Bucket for MergeBatcherWrapper<D, T, R>
where
    D: MzData + Ord + Data,
    T: MzData + Ord + PartialOrder + Data + BucketTimestamp,
    R: MzData + Semigroup + Default,
{
    type Timestamp = T;

    fn split(mut self, timestamp: &Self::Timestamp, fuel: &mut i64) -> (Self, Self) {
        // The implementation isn't tuned for performance. We should not bounce in and out of
        // different containers when not needed. The merge batcher we use can only accept
        // vectors as inputs, but not any other container type.
        // TODO: Allow the merge batcher to accept more generic containers.
        let upper = Antichain::from_elem(timestamp.clone());
        let mut lower = Self::new(self.logger.clone(), self.operator_id);
        let mut buffer = Vec::new();
        for chunk in self.inner.seal::<CapturingBuilder<_, _>>(upper) {
            *fuel = fuel.saturating_sub(chunk.len().try_into().expect("must fit"));
            // TODO: Avoid this cloning.
            buffer.extend(chunk.into_iter().cloned());
            lower.inner.push_container(&mut buffer);
            buffer.clear();
        }
        (lower, self)
    }
}

struct CapturingBuilder<D, T>(D, PhantomData<T>);

impl<D, T: Timestamp> Builder for CapturingBuilder<D, T> {
    type Input = D;
    type Time = T;
    type Output = Vec<D>;

    fn with_capacity(_keys: usize, _vals: usize, _upds: usize) -> Self {
        // Not needed for this implementation.
        unimplemented!()
    }

    fn push(&mut self, _chunk: &mut Self::Input) {
        // Not needed for this implementation.
        unimplemented!()
    }

    fn done(self, _description: Description<Self::Time>) -> Self::Output {
        // Not needed for this implementation.
        unimplemented!()
    }

    #[inline]
    fn seal(chain: &mut Vec<Self::Input>, _description: Description<Self::Time>) -> Self::Output {
        std::mem::take(chain)
    }
}
