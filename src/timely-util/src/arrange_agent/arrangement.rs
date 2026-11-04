// Copyright (c) 2015 Frank McSherry
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
//
// Portions of this file are derived from `arrange_core` in
// differential-dataflow 0.25.1. The original source code was retrieved from:
//
//     https://github.com/TimelyDataflow/differential-dataflow/blob/6e53ac674feacac83b0c3c5d123bdd5db488ef9d/differential-dataflow/src/operators/arrange/arrangement.rs
//
// The original source code is subject to the terms of the MIT license, a copy
// of which can be found in the LICENSE file at the root of this repository.

//! A copy of differential's `arrange_core` that builds the agent through a factory.
//!
//! The only intended deviation from upstream is agent construction: the operator obtains its
//! agent and writer from the factory instead of building a `TraceAgent`. A later
//! differential-dataflow bump can re-derive this file by diffing it against upstream.

use timely::Container;
use timely::container::{ContainerBuilder, PushInto};
use timely::dataflow::Stream;
use timely::dataflow::channels::pact::ParallelizationContract;
use timely::dataflow::operators::Capability;
use timely::dataflow::operators::generic::Operator;
use timely::order::PartialOrder;
use timely::progress::Antichain;
use timely::progress::Timestamp;

use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::{Batcher, Builder};

use crate::arrange_agent::{Agent, ArrangeContext, Writer};

/// As differential's `arrange_core`, maintaining the trace behind the agent and writer `factory`
/// returns.
///
/// `factory` is called once, when the operator is built. See [`Agent`] for the contract.
pub fn arrange_core_with<'scope, P, C, Chu, Ba, Bu, A, F>(
    stream: Stream<'scope, A::Time, C>,
    pact: P,
    name: &str,
    factory: F,
) -> Arranged<'scope, A>
where
    C: Container + Clone + 'static,
    P: ParallelizationContract<A::Time, C>,
    Chu: ContainerBuilder<Container = Ba::Output> + for<'a> PushInto<&'a mut C> + 'static,
    Ba: Batcher<Time = A::Time> + 'static,
    Bu: Builder<Time = A::Time, Input = Ba::Output, Output = A::Batch>,
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

    // fabricate a data-parallel operator using the `unary_notify` pattern.
    let reader_ref = &mut reader;
    let scope = stream.scope();

    let stream = stream.unary_frontier(pact, name, move |_capability, info| {
        let context = ArrangeContext::new(&scope, info);

        // Where we will deposit received updates, and from which we extract batches.
        let mut batcher = Ba::new(context.logger.clone(), context.info.global_id);

        // Capabilities for the lower envelope of updates in `batcher`.
        let mut capabilities = Antichain::<Capability<A::Time>>::new();

        let (reader_local, mut writer) = factory(context);

        *reader_ref = Some(reader_local);

        // Initialize to the minimal input frontier.
        let mut prev_frontier = Antichain::from_elem(<A::Time as Timestamp>::minimum());

        let mut chunker = Chu::default();

        move |(input, frontier), output| {
            // As we receive data, we need to (i) stash the data and (ii) keep *enough* capabilities.
            // We don't have to keep all capabilities, but we need to be able to form output messages
            // when we realize that time intervals are complete.

            input.for_each(|cap, data| {
                capabilities.insert(cap.retain(0));
                chunker.push_into(data);
                while let Some(chunk) = chunker.extract() {
                    batcher.push_into(std::mem::take(chunk));
                }
            });

            // The frontier may have advanced by multiple elements, which is an issue because
            // timely dataflow currently only allows one capability per message. This means we
            // must pretend to process the frontier advances one element at a time, batching
            // and sending smaller bites than we might have otherwise done.

            // Assert that the frontier never regresses.
            assert!(PartialOrder::less_equal(
                &prev_frontier.borrow(),
                &frontier.frontier()
            ));

            // Test to see if strict progress has occurred, which happens whenever the new
            // frontier isn't equal to the previous. It is only in this case that we have any
            // data processing to do.
            if prev_frontier.borrow() != frontier.frontier() {
                // Flush any data the chunker is still accumulating into the batcher before we
                // seal. The batcher only sees chunks the chunker has emitted; without this drain
                // a partial final chunk would never reach the batcher.
                while let Some(chunk) = chunker.finish() {
                    batcher.push_into(std::mem::take(chunk));
                }

                // There are two cases to handle with some care:
                //
                // 1. If any held capabilities are not in advance of the new input frontier,
                //    we must carve out updates now in advance of the new input frontier and
                //    transmit them as batches, which requires appropriate *single* capabilities;
                //    Until timely dataflow supports multiple capabilities on messages, at least.
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
                    let mut upper = Antichain::new(); // re-used allocation for sealing batches.

                    // For each capability not in advance of the input frontier ...
                    for (index, capability) in capabilities.elements().iter().enumerate() {
                        if !frontier.less_equal(capability.time()) {
                            // Assemble the upper bound on times we can commit with this capabilities.
                            // We must respect the input frontier, and *subsequent* capabilities, as
                            // we are pretending to retire the capability changes one by one.
                            upper.clear();
                            for time in frontier.frontier().iter() {
                                upper.insert(time.clone());
                            }
                            for other_capability in &capabilities.elements()[(index + 1)..] {
                                upper.insert(other_capability.time().clone());
                            }

                            // Extract updates not in advance of `upper`.
                            let (mut chain, description) = batcher.seal(upper.clone());
                            let batch = Bu::seal(&mut chain, description);

                            writer.insert(batch.clone(), Some(capability.time().clone()));

                            // send the batch to downstream consumers, empty or not.
                            output.session(&capabilities.elements()[index]).give(batch);
                        }
                    }

                    // Having extracted and sent batches between each capability and the input frontier,
                    // we should downgrade all capabilities to match the batcher's lower update frontier.
                    // This may involve discarding capabilities, which is fine as any new updates arrive
                    // in messages with new capabilities.

                    let mut new_capabilities = Antichain::new();
                    for time in batcher.frontier().iter() {
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
                    // Announce progress updates, even without data. We seal the batcher to
                    // advance its lower bound and frontier, but discard the readied updates
                    // rather than building a batch we would immediately drop.
                    let _ = batcher.seal(frontier.frontier().to_owned());
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
