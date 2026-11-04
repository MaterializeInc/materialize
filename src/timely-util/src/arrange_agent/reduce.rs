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
// Portions of this file are derived from the reduce driver in
// differential-dataflow 0.25.1. The original source code was retrieved from:
//
//     https://github.com/TimelyDataflow/differential-dataflow/blob/6e53ac674feacac83b0c3c5d123bdd5db488ef9d/differential-dataflow/src/operators/reduce.rs
//
// The original source code is subject to the terms of the MIT license, a copy
// of which can be found in the LICENSE file at the root of this repository.

//! A copy of differential's `reduce_with_tactic` driver that builds the output agent through a
//! factory.
//!
//! The only intended deviation from upstream is agent construction: the operator obtains its
//! output agent and writer from the factory instead of building a `TraceAgent`. A later
//! differential-dataflow bump can re-derive this file by diffing it against upstream.

use differential_dataflow::Data;

use std::marker::PhantomData;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::CapabilitySet;
use timely::dataflow::operators::Operator;
use timely::progress::Timestamp;
use timely::progress::frontier::Antichain;

use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::operators::reduce::ReduceTactic;
use differential_dataflow::trace::cursor::cursor_list;
use differential_dataflow::trace::implementations::containers::BatchContainer;
use differential_dataflow::trace::{
    BatchCursor, BatchDiff, BatchReader, BatchVal, BatchValOwn, Builder, Cursor, Description,
    Navigable, TraceReader,
};

use crate::arrange_agent::{Agent, ArrangeContext, Writer};

/// Sort and deduplicate a list.
#[inline(never)]
pub(crate) fn sort_dedup<T: Ord>(list: &mut Vec<T>) {
    list.dedup();
    list.sort();
    list.dedup();
}

/// As differential's `reduce_trace`, maintaining the output trace behind the agent and writer
/// `factory` returns.
///
/// `factory` is called once, when the operator is built. See [`reduce_with_tactic_with`].
pub fn reduce_trace_with<'scope, Tr1, Bu, A, KC, L, P, F>(
    trace: Arranged<'scope, Tr1>,
    name: &str,
    logic: L,
    push: P,
    factory: F,
) -> Arranged<'scope, A>
where
    Tr1: TraceReader<Batch: Navigable> + 'static,
    A: Agent<Batch: Navigable, Time = Tr1::Time> + 'static,
    KC: BatchContainer,
    BatchCursor<Tr1>: Cursor<Time = Tr1::Time, KeyContainer = KC>,
    for<'a> BatchCursor<Tr1>: Cursor<Key<'a> = KC::ReadItem<'a>>,
    for<'a> BatchCursor<A>: Cursor<Key<'a> = KC::ReadItem<'a>, ValOwn: Data, Time = A::Time>,
    Bu: Builder<Time = A::Time, Output = A::Batch, Input: Default> + 'static,
    L: FnMut(
            KC::ReadItem<'_>,
            &[(BatchVal<'_, Tr1>, BatchDiff<Tr1>)],
            &mut Vec<(BatchValOwn<A>, BatchDiff<A>)>,
            &mut Vec<(BatchValOwn<A>, BatchDiff<A>)>,
        ) + 'static,
    P: FnMut(&mut Bu::Input, KC::ReadItem<'_>, &mut Vec<(BatchValOwn<A>, A::Time, BatchDiff<A>)>)
        + 'static,
    F: FnOnce(ArrangeContext) -> (A, A::Writer),
{
    reduce_with_tactic_with(
        trace,
        name,
        cursors::CursorTactic::<Tr1::Batch, A::Batch, Bu, L, P>::new(logic, push),
        factory,
    )
}

/// As differential's `reduce_with_tactic`, maintaining the output trace behind the agent and
/// writer `factory` returns.
///
/// `factory` is called once, when the operator is built. The operator reads its prior output
/// through its clone of the agent, and returns another clone in the `Arranged`. See [`Agent`]
/// for the contract.
#[expect(
    clippy::debug_assert_with_mut_call,
    reason = "upstream's tiling check mutates only a local of the assertion"
)]
pub fn reduce_with_tactic_with<'scope, Tr1, A, T, F>(
    trace: Arranged<'scope, Tr1>,
    name: &str,
    mut tactic: T,
    factory: F,
) -> Arranged<'scope, A>
where
    Tr1: TraceReader + 'static,
    A: Agent<Time = Tr1::Time> + 'static,
    T: ReduceTactic<Tr1::Batch, A::Batch> + 'static,
    F: FnOnce(ArrangeContext) -> (A, A::Writer),
{
    let mut result_trace = None;

    // fabricate a data-parallel operator using the `unary_notify` pattern.
    let stream = {
        let mut source_trace = trace.trace;
        let result_trace = &mut result_trace;
        let scope = trace.stream.scope();
        trace
            .stream
            .unary_frontier(Pipeline, name, move |_capability, operator_info| {
                let (mut output_reader, mut output_writer) =
                    factory(ArrangeContext::new(&scope, operator_info));

                *result_trace = Some(output_reader.clone());

                // Capabilities for the lower envelope of the interesting times the operator holds.
                let mut capabilities = CapabilitySet::<Tr1::Time>::default();

                // Upper and lower frontiers for the pending input and output batches to process.
                let mut upper_limit = Antichain::from_elem(<Tr1::Time as Timestamp>::minimum());
                let mut lower_limit = Antichain::from_elem(<Tr1::Time as Timestamp>::minimum());

                move |(input, frontier), output| {
                    // The operator receives input batches, which it treats as contiguous and will
                    // collect and then process as one batch. It captures the input frontier from
                    // the batches, from the upstream trace, and from the input frontier, and
                    // retires the work through that interval.
                    //
                    // Reduce may retain capabilities and need to perform work and produce output at
                    // times that may not be seen in its input. The standard example is that updates
                    // at `(0, 1)` and `(1, 0)` may result in outputs at `(1, 1)` as well, even with
                    // no input at that time.

                    let mut batch_storage = Vec::new();

                    // Downgrade previous upper limit to be current lower limit.
                    lower_limit.clear();
                    lower_limit.extend(upper_limit.borrow().iter().cloned());

                    // Drain input batches in order, capturing capabilities and the last upper.
                    input.for_each(|capability, batches| {
                        capabilities.insert(capability.retain(0));
                        for batch in batches.drain(..) {
                            upper_limit.clone_from(batch.upper());
                            batch_storage.push(batch);
                        }
                    });

                    // Pull in any subsequent empty batches we believe to exist.
                    source_trace.advance_upper(&mut upper_limit);
                    // Incorporate the input frontier guarantees as well.
                    let mut joined = Antichain::new();
                    differential_dataflow::lattice::antichain_join_into(
                        &upper_limit.borrow()[..],
                        &frontier.frontier()[..],
                        &mut joined,
                    );
                    upper_limit = joined;

                    // We plan to retire the interval [lower_limit, upper_limit), which should be
                    // non-empty to proceed.
                    if upper_limit != lower_limit {
                        // Acquire the pre-existing input and output batches preceding the interval.
                        // Batch handles are cheap to clone, so we fetch them whether or not the
                        // tactic finds work to do.
                        let source_batches = source_trace
                            .batches_through(lower_limit.borrow())
                            .expect("failed to acquire source batches");
                        let output_batches = output_reader
                            .batches_through(lower_limit.borrow())
                            .expect("failed to acquire output batches");

                        // The times the operator currently holds capabilities for, as an antichain.
                        let held: Antichain<Tr1::Time> =
                            capabilities.iter().map(|c| c.time().clone()).collect();

                        // Retire the interval. The tactic reasons only about times: it returns
                        // output batches each tagged with the time to ship it at, and the new
                        // frontier of interesting times.
                        let (produced, new_frontier) = tactic.retire(
                            source_batches,
                            output_batches,
                            batch_storage,
                            &lower_limit,
                            &upper_limit,
                            &held,
                        );

                        // Contract checks (see `ReduceTactic::retire`). Cheap, debug-only.
                        debug_assert!(
                            produced
                                .iter()
                                .all(|(time, _)| held.elements().contains(time)),
                            "ReduceTactic::retire shipped a batch at a time \
                             not held as a capability",
                        );
                        debug_assert!(
                            {
                                // Ordered output makes tiling a single linear scan: each
                                // description's lower must meet the previous upper (starting at
                                // `lower_limit`), ending at `upper_limit`.
                                let mut edge = lower_limit.clone();
                                let abutting = produced.iter().all(|(_, batch)| {
                                    let matches = batch.description().lower() == &edge;
                                    edge.clone_from(batch.description().upper());
                                    matches
                                });
                                abutting && (produced.is_empty() || edge == upper_limit)
                            },
                            "ReduceTactic::retire output must be ordered \
                             and tile [lower, upper)",
                        );

                        // Ship each batch at a capability minted from the set at its time, and
                        // commit it to the output trace. The times are elements of `held`, so they
                        // stay valid until we downgrade.
                        for (time, batch) in produced {
                            let capability = capabilities.delayed(&time);
                            output.session(&capability).give(batch.clone());
                            output_writer.insert(batch, Some(time));
                        }

                        // Downgrade to the frontier the tactic handed back (a no-op when it found
                        // no work).
                        capabilities.downgrade(new_frontier);

                        // ensure that observed progress is reflected in the output.
                        output_writer.seal(upper_limit.clone());

                        // We only anticipate future times in advance of `upper_limit`.
                        source_trace.set_logical_compaction(upper_limit.borrow());
                        output_reader.set_logical_compaction(upper_limit.borrow());

                        // We will only slice the data between future batches.
                        source_trace.set_physical_compaction(upper_limit.borrow());
                        output_reader.set_physical_compaction(upper_limit.borrow());
                    }

                    // Exert trace maintenance if we have been so requested.
                    output_writer.exert();
                }
            })
    };

    Arranged {
        stream,
        trace: result_trace.unwrap(),
    }
}

/// The conventional cursor-based [`ReduceTactic`].
///
/// It builds a [`CursorList`](differential_dataflow::trace::cursor::CursorList) over the input, output, and new-batch
/// updates and replays them together per key, applying `logic` and shaping output with `push`. It holds
/// the outstanding synthetic interesting `(key, time)` moments across activations, and reasons only
/// about times: capabilities are the driver's concern.
mod cursors;
