// Copyright (c) 2015 Frank McSherry
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
//
// Portions of this file are derived from `arrange_core` in
// differential-dataflow. The original source code was retrieved from:
//
//     https://github.com/TimelyDataflow/differential-dataflow/blob/6074cc78884bdc169c5219af7fdbd76da7009cba/differential-dataflow/src/operators/arrange/arrangement.rs
//
// The crates.io release 0.26.0, which the workspace depends on, ships these sources unchanged.
//
// The original source code is subject to the terms of the MIT license, a copy
// of which can be found in the LICENSE file at the root of this repository.
//
// Not upstream: `crate::` paths name `differential_dataflow::`.

//! A copy of differential's `arrange_core` that builds the agent through a factory.
//!
//! The only intended deviation from upstream is agent construction, marked `Not upstream:`.
//! The operator obtains its agent and writer from the factory instead of building a
//! `TraceAgent`.

use timely::Container;
use timely::dataflow::Stream;
use timely::dataflow::channels::pact::ParallelizationContract;
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::operators::{Capability, CapabilitySet};
use timely::order::PartialOrder;
use timely::progress::Antichain;
use timely::progress::Stamp;
use timely::progress::Timestamp;

use differential_dataflow::batcher::Batcher;
use differential_dataflow::logging::Logger;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::{self, Description};

use super::{Agent, ArrangeContext, Writer};

/// Arranges a stream of updates by a key, configured with a name and a parallelization contract.
///
/// This operator arranges a stream of values into a shared trace, whose contents it maintains.
/// It uses the supplied parallelization contract to distribute the data, which does not need to
/// be consistently by key (though this is the most common).
///
/// `factory` is called once, when the operator is built, and returns the agent and the writer
/// that maintain the trace. See [`Agent`] for the contract.
// Not upstream: named `arrange_core_with`, generic over the agent `A` and its `factory`
// instead of the trace `Tr`.
pub fn arrange_core_with<'scope, P, C, Ba, A, F>(
    stream: Stream<'scope, A::Time, C>,
    pact: P,
    name: &str,
    batcher: impl FnOnce(Option<Logger>, usize) -> Ba,
    factory: F,
) -> Arranged<'scope, A>
where
    C: Container + Clone + 'static,
    P: ParallelizationContract<A::Time, C>,
    Ba: Batcher<C, Time = A::Time, Output: Into<A::Batch>> + 'static,
    A: Agent + 'static,
    F: FnOnce(ArrangeContext) -> (A, A::Writer),
{
    // The `Arrange` operator is tasked with reacting to an advancing input
    // frontier by producing the sequence of batches whose lower and upper
    // bounds are those frontiers, containing updates at times greater or
    // equal to lower and not greater or equal to upper.
    //
    // The operator uses its batch type's `Batcher`, which accepts update
    // triples and responds to requests to "seal" batches (presented as new
    // upper frontiers).
    //
    // Each sealed batch is presented to the trace, and if at all possible
    // transmitted along the outgoing channel. Empty batches may not have
    // a corresponding capability, as they are only retained for actual data
    // held by the batcher, which may prevents the operator from sending an
    // empty batch.

    let mut reader: Option<A> = None;

    // fabricate a data-parallel operator that holds capabilities and consults its input frontier.
    let reader_ref = &mut reader;
    let scope = stream.scope();

    let stream = stream.unary_frontier(pact, name, move |_capability, info| {
        // Not upstream: the context acquires the logger for arrange events, the activator, and
        // the default exertion logic, and the factory builds the empty trace and its agent.
        let context = ArrangeContext::new(&scope, info);

        // Where we will deposit received updates, and from which we extract batches.
        let mut batcher = batcher(context.logger.clone(), context.info.global_id);

        // Capabilities for the lower envelope of updates in `batcher`.
        let mut capabilities = Antichain::<Capability<A::Time>>::new();

        let (reader_local, mut writer) = factory(context);

        *reader_ref = Some(reader_local);

        // Initialize to the minimal input frontier.
        let mut prev_frontier = Antichain::from_elem(A::Time::minimum());

        move |(input, frontier), output| {
            // As we receive data, we need to (i) stash the data and (ii) keep *enough* capabilities.
            // We don't have to keep all capabilities, but we need to be able to form output messages
            // when we realize that time intervals are complete.

            input.for_each(|cap, data| {
                for capability in cap.retain_stamp(0).iter() {
                    capabilities.insert(capability.clone());
                }
                batcher.insert(data);
            });

            // The frontier may have advanced by multiple elements. We extract one batch for the
            // whole advance, and send it stamped with every held capability it retires.

            // Assert that the frontier never regresses.
            assert!(PartialOrder::less_equal(
                &prev_frontier.borrow(),
                &frontier.frontier()
            ));

            // Test to see if strict progress has occurred, which happens whenever the new
            // frontier isn't equal to the previous. It is only in this case that we have any
            // data processing to do.
            if prev_frontier.borrow() != frontier.frontier() {
                // There are two cases to handle with some care:
                //
                // 1. If any held capabilities are not in advance of the new input frontier,
                //    we must carve out updates now in advance of the new input frontier and
                //    transmit them as a batch, stamped with the capabilities they retire.
                //
                // 2. If there are no held capabilities in advance of the new input frontier,
                //    then there are no updates not in advance of the new input frontier and
                //    we can simply create an empty input batch with the new upper frontier
                //    and feed this to the trace agent (but not along the timely output).

                // If there is at least one capability not in advance of the input frontier ...
                if capabilities
                    .elements()
                    .iter()
                    .any(|c| !frontier.less_equal(c.time()))
                {
                    // The capabilities to retire: those not in advance of the input frontier.
                    // Each update extracted below is greater or equal to one of them, as updates
                    // supported only by the remaining capabilities are in advance of the input
                    // frontier and remain in the batcher.
                    let retired = capabilities
                        .elements()
                        .iter()
                        .filter(|c| !frontier.less_equal(c.time()))
                        .cloned()
                        .collect::<CapabilitySet<_>>();

                    // Extract all updates not in advance of the input frontier, as one batch.
                    // The batch spans the interval from the previously reported frontier to the
                    // current one, which is exactly the interval the batcher carves out.
                    let description = Description::new(
                        prev_frontier.clone(),
                        frontier.frontier().to_owned(),
                        Antichain::from_elem(A::Time::minimum()),
                    );
                    let (extracted, retained) = batcher.extract(frontier.frontier());

                    let batch = trace::Span::new(description, extracted.map(Into::into));

                    let stamp = retired
                        .iter()
                        .map(|c| c.time().clone())
                        .collect::<Stamp<_>>();
                    writer.insert(batch.clone(), stamp);

                    // send the batch to downstream consumers, empty or not.
                    output.session(&retired).give(batch);

                    // Having extracted and sent the batch of updates not in advance of the input
                    // frontier, we downgrade all capabilities to match the batcher's lower update
                    // frontier.
                    // This may involve discarding capabilities, which is fine as any new updates arrive
                    // in messages with new capabilities.
                    let mut new_capabilities = Antichain::new();
                    for time in retained.iter() {
                        if let Some(capability) = capabilities
                            .elements()
                            .iter()
                            .find(|c| c.time().less_equal(time))
                        {
                            new_capabilities.insert(capability.delayed(time));
                        } else {
                            panic!("failed to find capability");
                        }
                    }
                    capabilities = new_capabilities;
                } else {
                    // Announce progress updates, even without data. No held capability precedes
                    // the input frontier, so no update does either, and the batcher has nothing
                    // to extract.
                    writer.seal(frontier.frontier().to_owned());
                }

                prev_frontier.clear();
                prev_frontier.extend(frontier.frontier().iter().cloned());
            }

            writer.exert();
        }
    });

    Arranged {
        stream,
        trace: reader.unwrap(),
    }
}
