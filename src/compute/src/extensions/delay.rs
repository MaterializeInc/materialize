// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub use stream::Delay as DelayStream;

mod stream {
    use crate::sink::ConsolidatingVec;
    use mz_compute_types::dyncfgs::CONSOLIDATING_VEC_GROWTH_DAMPENER;
    use mz_dyncfg::ConfigSet;
    use mz_timely_util::temporal::{BucketChain, BucketTimestamp, Storage};
    use timely::container::{ContainerBuilder, PushInto};
    use timely::dataflow::channels::pact::Pipeline;
    use timely::dataflow::operators::Operator;
    use timely::dataflow::{Scope, StreamCore};
    use timely::Data;

    pub trait Delay<G: Scope, O> {
        fn delay<CB>(&self, config: &ConfigSet) -> StreamCore<G, CB::Container>
        where
            CB: ContainerBuilder + PushInto<O>;
    }

    impl<D: Ord, T: BucketTimestamp> Storage for ConsolidatingVec<(D, T)> {
        type Timestamp = T;

        fn split(self, timestamp: &Self::Timestamp, fuel: &mut isize) -> (Self, Self) {
            *fuel = fuel.saturating_sub(self.len().try_into().expect("must fit"));
            let mut left = ConsolidatingVec::new(128, self.growth_dampener());
            let mut right = ConsolidatingVec::new(128, self.growth_dampener());
            for ((data, time), diff) in self.into_iter() {
                if &time < timestamp {
                    left.push(((data, time), diff));
                } else {
                    right.push(((data, time), diff));
                }
            }
            (left, right)
        }
    }

    impl<G, D> Delay<G, (D, G::Timestamp, mz_repr::Diff)>
        for StreamCore<G, Vec<(D, G::Timestamp, mz_repr::Diff)>>
    where
        G: Scope,
        G::Timestamp: BucketTimestamp,
        D: Data + Ord,
    {
        fn delay<CB>(&self, config: &ConfigSet) -> StreamCore<G, CB::Container>
        where
            CB: ContainerBuilder + PushInto<(D, G::Timestamp, mz_repr::Diff)>,
        {
            let growth_dampener = CONSOLIDATING_VEC_GROWTH_DAMPENER.get(config);
            let mut chain = BucketChain::new(ConsolidatingVec::new(128, growth_dampener));
            self.unary_frontier::<CB, _, _, _>(Pipeline, "Delay", |cap, _| {
                let mut cap = Some(cap);
                move |input, output| {
                    while let Some((_time, data)) = input.next() {
                        for (datum, time, diff) in data.drain(..) {
                            chain
                                .find_mut(&time)
                                .expect("must exist")
                                .push(((datum, time), diff));
                        }
                    }
                    let peeled = chain.peel(input.frontier().frontier());
                    let time = cap.as_ref().expect("must exist");
                    let mut session = output.session_with_builder(&time);
                    for ((datum, time), diff) in peeled.into_iter().flatten() {
                        session.give((datum, time, diff));
                    }
                    if input.frontier().is_empty() {
                        cap = None;
                    } else {
                        if let Some(cap) = cap.as_mut() {
                            cap.downgrade(&input.frontier().frontier()[0]);
                        }
                    }
                }
            })
        }
    }
}
