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

use differential_dataflow::containers::TimelyStack;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::trace::implementations::chunker::ColumnationChunker;
use differential_dataflow::trace::implementations::merge_batcher::{ColMerger, MergeBatcher};
use differential_dataflow::trace::{Batcher, Builder, Description};
use mz_timely_util::temporal::{BucketChain, BucketTimestamp, Storage};
use timely::container::{ContainerBuilder, PushInto};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;
use timely::dataflow::{Scope, StreamCore};
use timely::progress::{Antichain, Timestamp};
use timely::{Data, PartialOrder};

use crate::typedefs::MzData;

/// Sort outstanding updates into a [`BucketChain`], and reveal data not in advance of the frontier.
pub trait TemporalBucketing<G: Scope, O> {
    /// Construct a new stream that stores updates into a [`BucketChain`] and reveals data
    /// not in advance of the frontier.
    fn bucket<CB>(&self) -> StreamCore<G, CB::Container>
    where
        CB: ContainerBuilder + PushInto<O>;
}

impl<G, D> TemporalBucketing<G, (D, G::Timestamp, mz_repr::Diff)>
    for StreamCore<G, Vec<(D, G::Timestamp, mz_repr::Diff)>>
where
    G: Scope,
    G::Timestamp: Data + MzData + BucketTimestamp,
    D: Data + MzData + Ord + std::fmt::Debug,
{
    fn bucket<CB>(&self) -> StreamCore<G, CB::Container>
    where
        CB: ContainerBuilder + PushInto<(D, G::Timestamp, mz_repr::Diff)>,
    {
        let scope = self.scope();
        let logger = scope.logger_for("differential/arrange").map(Into::into);

        self.unary_frontier::<CB, _, _, _>(Pipeline, "Delay", |cap, info| {
            let mut chain = BucketChain::new(MergeBatcherWrapper::new(logger, info.global_id));
            let activator = scope.activator_for(info.address);
            // Cap tracking the lower bound of potentially outstanding data.
            let mut cap = Some(cap);
            let mut buffer = Vec::new();
            move |input, output| {
                while let Some((_time, data)) = input.next() {
                    // Sort data by time, then iterate and
                    data.sort_by(|(_, t, _), (_, t2, _)| t.cmp(t2));
                    let mut index = None;
                    for (datum, time, diff) in data.drain(..) {
                        let this_index = chain.index_of(&time).unwrap();
                        if let Some(index) = index
                            && index != this_index
                        {
                            chain.index_mut(index).inner.push_container(&mut buffer);
                        }
                        index = Some(this_index);
                        buffer.push((datum, time, diff));
                    }
                    // Handle leftover data in the buffer.
                    if let Some(index) = index
                        && !buffer.is_empty()
                    {
                        chain.index_mut(index).inner.push_container(&mut buffer);
                    }
                    buffer.clear();
                }

                // Check for data that is ready to be revealed.
                let peeled = chain.peel(input.frontier().frontier());
                if let Some(cap) = cap.as_ref() {
                    let mut session = output.session_with_builder(cap);
                    for stack in peeled.into_iter().flat_map(|x| x.done()) {
                        session.give_iterator(stack.iter().cloned());
                    }
                } else {
                    assert_eq!(peeled.into_iter().flat_map(|x| x.done()).next(), None);
                }

                // Downgrade the cap to the current frontier.
                if input.frontier().is_empty() {
                    cap = None;
                } else if let Some(cap) = cap.as_mut() {
                    cap.downgrade(&input.frontier().frontier()[0]);
                }

                // Maintain the bucket chain by restoring it with fuel.
                let mut fuel = 1000;
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

impl<D, T, R> Storage for MergeBatcherWrapper<D, T, R>
where
    D: MzData + Ord + Data,
    T: MzData + Ord + PartialOrder + Data + BucketTimestamp,
    R: MzData + Semigroup + Default,
{
    type Timestamp = T;

    fn split(mut self, timestamp: &Self::Timestamp, fuel: &mut isize) -> (Self, Self) {
        // The implementation isn't tuned for performance. We should not bounce in and out of
        // different containers when not needed.
        let upper = Antichain::from_elem(timestamp.clone());
        let mut lower = Self::new(self.logger.clone(), self.operator_id);
        let mut buffer = Vec::new();
        for chunk in self.inner.seal::<CapturingBuilder<_, _>>(upper) {
            *fuel = fuel.saturating_sub(chunk.len().try_into().expect("must fit"));
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
