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
// Portions of this file are derived from `reduce_trace` and `reduce_with_tactic` in
// differential-dataflow. The original source code was retrieved from:
//
//     https://github.com/TimelyDataflow/differential-dataflow/blob/6074cc78884bdc169c5219af7fdbd76da7009cba/differential-dataflow/src/operators/cursor/reduce.rs
//     https://github.com/TimelyDataflow/differential-dataflow/blob/6074cc78884bdc169c5219af7fdbd76da7009cba/differential-dataflow/src/operators/reduce.rs
//
// The original source code is subject to the terms of the MIT license, a copy
// of which can be found in the LICENSE file at the root of this repository.

//! Copies of differential's `reduce_trace` and `reduce_with_tactic` that build the output agent
//! through a factory.
//!
//! The only intended deviation from upstream is agent construction, marked `Not upstream:`.
//! The operator obtains its output agent and writer from the factory instead of building a
//! `TraceAgent`. `reduce_trace_with` uses differential's public `CursorTactic`. The function
//! bodies keep upstream's formatting so that a later differential-dataflow bump can re-derive
//! this file by diffing it against upstream.

use timely::progress::frontier::Antichain;
use timely::progress::Timestamp;
use timely::dataflow::operators::Operator;
use timely::dataflow::operators::CapabilitySet;
use timely::dataflow::channels::pact::Pipeline;

use differential_dataflow::Data;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::operators::cursor::reduce::CursorTactic;
use differential_dataflow::operators::reduce::ReduceTactic;
use differential_dataflow::trace::{BatchCursor, BatchDiff, BatchVal, BatchValOwn, Builder, Cursor, Navigable, TraceReader};
use differential_dataflow::trace::implementations::containers::BatchContainer;

use super::{Agent, ArrangeContext, Writer};

/// A key-wise reduction of values in an input trace.
///
/// This method exists to provide reduce functionality without opinions about qualifying trace types.
///
/// The `logic` closure is expected to take a key, accumulated input, and tentative accumulated output,
/// and populate its final argument with whatever it feels to be appopriate updates. The behavior and
/// correctness of the implementation rely on this making sense, and e.g. ideally the updates would if
/// applied to the tentative output bring it in line with some function applied to the input.
///
/// The `push` closure is expected to clear its first argument, then populate it with the key and drain
/// the value updates, as appropriate for the container. It is critical that it clear the container as
/// the operator has no ability to do this otherwise, and failing to do so represents a leak from one
/// key's computation to another, and will likely introduce non-determinism.
///
/// `factory` is called once, when the operator is built. See [`reduce_with_tactic_with`].
// Not upstream: named `reduce_trace_with`, generic over the output agent `A` and its `factory`
// instead of the output trace `Tr2`.
pub fn reduce_trace_with<'scope, Tr1, Bu, A, KC, L, P, F>(trace: Arranged<'scope, Tr1>, name: &str, logic: L, push: P, factory: F) -> Arranged<'scope, A>
where
    Tr1: TraceReader<Batch: Navigable> + 'static,
    A: Agent<Batch: Navigable, Time = Tr1::Time> + 'static,
    KC: BatchContainer,
    BatchCursor<Tr1>: Cursor<Time = Tr1::Time, KeyContainer = KC>,
    for<'a> BatchCursor<Tr1>: Cursor<Key<'a> = KC::ReadItem<'a>>,
    for<'a> BatchCursor<A>: Cursor<Key<'a> = KC::ReadItem<'a>, ValOwn: Data, Time = A::Time>,
    Bu: Builder<Time=A::Time, Output: Into<A::Batch>, Input: Default> + 'static,
    L: FnMut(KC::ReadItem<'_>, &[(BatchVal<'_, Tr1>, BatchDiff<Tr1>)], &mut Vec<(BatchValOwn<A>, BatchDiff<A>)>, &mut Vec<(BatchValOwn<A>, BatchDiff<A>)>)+'static,
    P: FnMut(&mut Bu::Input, KC::ReadItem<'_>, &mut Vec<(BatchValOwn<A>, A::Time, BatchDiff<A>)>) + 'static,
    F: FnOnce(ArrangeContext) -> (A, A::Writer),
{
    reduce_with_tactic_with(trace, name, CursorTactic::<Tr1::Batch, A::Batch, Bu, L, P>::new(logic, push), factory)
}

/// Drives a key-wise reduction using a supplied [`ReduceTactic`].
///
/// This is the general reduce operator: it does the dataflow plumbing (frontiers, capabilities, output
/// trace maintenance) and routes the per-interval work through the tactic. It requires only
/// `TraceReader` of its input and `Trace` of its output, never `Navigable`: it extracts batches via
/// `spans_through`, and building cursors over them (if that is how the reduce proceeds) is the
/// tactic's concern.
///
/// `factory` is called once, when the operator is built. The operator reads its prior output
/// through its clone of the agent, and returns another clone in the `Arranged`. See [`Agent`]
/// for the contract.
// Not upstream: named `reduce_with_tactic_with`, generic over the output agent `A` and its
// `factory` instead of the output trace `Tr2`.
pub fn reduce_with_tactic_with<'scope, Tr1, A, T, F>(trace: Arranged<'scope, Tr1>, name: &str, mut tactic: T, factory: F) -> Arranged<'scope, A>
where
    Tr1: TraceReader + 'static,
    A: Agent<Time = Tr1::Time> + 'static,
    T: ReduceTactic<Tr1::Time, Tr1::Batch, A::Batch> + 'static,
    F: FnOnce(ArrangeContext) -> (A, A::Writer),
{
    let mut result_trace = None;

    // fabricate a data-parallel operator that holds capabilities and consults its input frontier.
    let stream = {

        let mut source_trace = trace.trace;
        let result_trace = &mut result_trace;
        let scope = trace.stream.scope();
        trace.stream.unary_frontier(Pipeline, name, move |_capability, operator_info| {

            // Not upstream: the context acquires the logger for arrange events, the activator,
            // and the default exertion logic, and the factory builds the empty trace and its
            // agent.
            let (mut output_reader, mut output_writer) = factory(ArrangeContext::new(&scope, operator_info));

            *result_trace = Some(output_reader.clone());

            // Capabilities for the lower envelope of the interesting times the operator holds.
            let mut capabilities = CapabilitySet::<Tr1::Time>::default();

            // Upper and lower frontiers for the pending input and output batches to process.
            let mut upper_limit = Antichain::from_elem(<Tr1::Time as Timestamp>::minimum());
            let mut lower_limit = Antichain::from_elem(<Tr1::Time as Timestamp>::minimum());
            // The same, but as bounds of source trace batches. The limits above join in the input frontier,
            // which can put them strictly inside a source batch, where the trace cannot be cut. No update lies
            // between the two, as the input frontier rules out the times the join adds.
            let mut source_upper = Antichain::from_elem(<Tr1::Time as Timestamp>::minimum());
            let mut source_lower = Antichain::from_elem(<Tr1::Time as Timestamp>::minimum());

            move |(input, frontier), output| {

                // The operator receives input batches, which it treats as contiguous and will collect and
                // then process as one batch. It captures the input frontier from the batches, from the upstream
                // trace, and from the input frontier, and retires the work through that interval.
                //
                // Reduce may retain capabilities and need to perform work and produce output at times that
                // may not be seen in its input. The standard example is that updates at `(0, 1)` and `(1, 0)`
                // may result in outputs at `(1, 1)` as well, even with no input at that time.

                let mut batch_storage = Vec::new();

                // Downgrade previous upper limit to be current lower limit.
                lower_limit.clear();
                lower_limit.extend(upper_limit.borrow().iter().cloned());
                source_lower.clone_from(&source_upper);

                // Drain input batches in order, capturing capabilities and the last upper.
                input.for_each(|capability, batches| {
                    for capability in capability.retain_stamp(0).iter() {
                        capabilities.insert(capability.clone());
                    }
                    for batch in batches.drain(..) {
                        source_upper.clone_from(batch.upper());
                        batch_storage.push(batch);
                    }
                });

                // Pull in any subsequent empty batches we believe to exist.
                source_trace.advance_upper(&mut source_upper);
                // Incorporate the input frontier guarantees as well.
                let mut joined = Antichain::new();
                differential_dataflow::lattice::antichain_join_into(&source_upper.borrow()[..], &frontier.frontier()[..], &mut joined);
                upper_limit = joined;

                // We plan to retire the interval [lower_limit, upper_limit), which should be non-empty to proceed.
                if upper_limit != lower_limit {

                    // Acquire the pre-existing input and output batches preceding the interval. Batch handles
                    // are cheap to clone, so we fetch them whether or not the tactic finds work to do.
                    let source_batches = source_trace.batches_through(source_lower.borrow()).expect("failed to acquire source batches");
                    let output_batches = output_reader.batches_through(lower_limit.borrow()).expect("failed to acquire output batches");

                    // The times the operator currently holds capabilities for, as an antichain.
                    let held: Antichain<Tr1::Time> = capabilities.iter().map(|c| c.time().clone()).collect();

                    // Retire the interval. The tactic reasons only about times: it returns the output
                    // batch to ship, if any, and the new frontier of interesting times.
                    let (produced, new_frontier) = tactic.retire(source_batches, output_batches, batch_storage.into_iter().filter_map(|b| b.inner).collect(), &lower_limit, &upper_limit, &held);

                    // Contract check (see `ReduceTactic::retire`). Cheap, debug-only.
                    debug_assert!(
                        produced.as_ref().is_none_or(|batch| batch.lower() == &lower_limit && batch.upper() == &upper_limit),
                        "ReduceTactic::retire output must span [lower, upper)",
                    );

                    // Ship the batch stamped with the capabilities it retires — those not in advance of
                    // the upper limit — and commit it to the output trace. The times are elements of
                    // `held`, so they stay valid until we downgrade.
                    if let Some(batch) = produced {
                        let retiring = capabilities
                            .iter()
                            .filter(|c| !upper_limit.less_equal(c.time()))
                            .cloned()
                            .collect::<CapabilitySet<_>>();
                        output.session(&retiring).give(batch.clone());
                        let stamp = retiring.iter().map(|c| c.time().clone()).collect::<timely::progress::Stamp<_>>();
                        output_writer.insert(batch, stamp);
                    }

                    // Downgrade to the frontier the tactic handed back (a no-op when it found no work).
                    capabilities.downgrade(new_frontier);

                    // ensure that observed progress is reflected in the output.
                    output_writer.seal(upper_limit.clone());

                    // We only anticipate future times in advance of `upper_limit`.
                    source_trace.set_logical_compaction(upper_limit.borrow());
                    output_reader.set_logical_compaction(upper_limit.borrow());

                    // We will only slice the data between future batches.
                    source_trace.set_physical_compaction(source_upper.borrow());
                    output_reader.set_physical_compaction(upper_limit.borrow());
                }

                // Exert trace maintenance if we have been so requested.
                output_writer.exert();
            }
        }
    )
    };

    Arranged { stream, trace: result_trace.unwrap() }
}
