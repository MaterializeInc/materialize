// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A fork of DD's `JoinCore::join_core`.
//!
//! Currently, compute rendering knows two implementations for linear joins:
//!
//!  * Differential's `JoinCore::join_core`
//!  * A Materialize fork thereof, called `mz_join_core`
//!
//! `mz_join_core` exists to solve a responsiveness problem with the DD implementation.
//! DD's join is only able to yield between keys. When computing a large cross-join or a highly
//! skewed join, this can result in loss of interactivity when the join operator refuses to yield
//! control for multiple seconds or longer, which in turn causes degraded user experience.
//!
//! `mz_join_core` currently fixes the yielding issue by omitting the merge-join matching strategy
//! implemented in DD's join implementation. This leaves only the nested loop strategy for which it
//! is easy to implement yielding within keys.
//!
//! While `mz_join_core` retains responsiveness in the face of cross-joins it is also, due to its
//! sole reliance on nested-loop matching, significantly slower than DD's join for workloads that
//! have a large amount of edits at different times. We consider these niche workloads for
//! Materialize today, due to the way source ingestion works, but that might change in the future.
//!
//! For the moment, we keep both implementations around, selectable through a feature flag.
//! We expect `mz_join_core` to be more useful in Materialize today, but being able to fall back to
//! DD's implementation provides a safety net in case that assumption is wrong.
//!
//! In the mid-term, we want to arrive at a single join implementation that is as efficient as DD's
//! join and as responsive as `mz_join_core`. Whether that means adding merge-join matching to
//! `mz_join_core` or adding better fueling to DD's join implementation is still TBD.

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::pin::Pin;
use std::rc::Rc;
use std::time::Instant;

use differential_dataflow::Data;
use differential_dataflow::IntoOwned;
use differential_dataflow::consolidation::consolidate_from;
use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::arrangement::Arranged;
use differential_dataflow::trace::{BatchReader, Cursor, TraceReader};
use mz_ore::future::yield_now;
use mz_repr::Diff;
use timely::PartialOrder;
use timely::container::{CapacityContainerBuilder, PushInto, SizableContainer};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::generic::OutputHandleCore;
use timely::dataflow::operators::{Capability, Operator};
use timely::dataflow::{Scope, StreamCore};
use timely::progress::timestamp::Timestamp;
use tracing::trace;

use crate::render::context::ShutdownProbe;

/// Joins two arranged collections with the same key type.
///
/// Each matching pair of records `(key, val1)` and `(key, val2)` are subjected to the `result` function,
/// which produces something implementing `IntoIterator`, where the output collection will have an entry for
/// every value returned by the iterator.
pub(super) fn mz_join_core<G, Tr1, Tr2, L, I, YFn, C>(
    arranged1: &Arranged<G, Tr1>,
    arranged2: &Arranged<G, Tr2>,
    shutdown_probe: ShutdownProbe,
    result: L,
    yield_fn: YFn,
) -> StreamCore<G, C>
where
    G: Scope,
    G::Timestamp: Lattice,
    Tr1: TraceReader<Time = G::Timestamp, Diff = Diff> + Clone + 'static,
    Tr2: for<'a> TraceReader<Key<'a> = Tr1::Key<'a>, Time = G::Timestamp, Diff = Diff>
        + Clone
        + 'static,
    L: FnMut(Tr1::Key<'_>, Tr1::Val<'_>, Tr2::Val<'_>) -> I + 'static,
    I: IntoIterator<Item: Data> + 'static,
    YFn: Fn(Instant, usize) -> bool + 'static,
    C: SizableContainer + PushInto<(I::Item, G::Timestamp, Diff)> + Data,
{
    let mut trace1 = arranged1.trace.clone();
    let mut trace2 = arranged2.trace.clone();

    arranged1.stream.binary_frontier(
        &arranged2.stream,
        Pipeline,
        Pipeline,
        "Join",
        move |capability, info| {
            let operator_id = info.global_id;

            // Acquire an activator to reschedule the operator when it has unfinished work.
            let activator = arranged1.stream.scope().activator_for(info.address);

            // Our initial invariants are that for each trace, physical compaction is less or equal the trace's upper bound.
            // These invariants ensure that we can reference observed batch frontiers from `_start_upper` onward, as long as
            // we maintain our physical compaction capabilities appropriately. These assertions are tested as we load up the
            // initial work for the two traces, and before the operator is constructed.

            // Acknowledged frontier for each input.
            // These two are used exclusively to track batch boundaries on which we may want/need to call `cursor_through`.
            // They will drive our physical compaction of each trace, and we want to maintain at all times that each is beyond
            // the physical compaction frontier of their corresponding trace.
            // Should we ever *drop* a trace, these are 1. much harder to maintain correctly, but 2. no longer used.
            use timely::progress::frontier::Antichain;
            let mut acknowledged1 = Antichain::from_elem(<G::Timestamp>::minimum());
            let mut acknowledged2 = Antichain::from_elem(<G::Timestamp>::minimum());

            // deferred work of batches from each input.
            let result_fn = Rc::new(RefCell::new(result));
            let yield_fn = Rc::new(yield_fn);
            let mut todo1 = Work::<<Tr1::Batch as BatchReader>::Cursor, Tr2::Cursor, _, _, _>::new(
                Rc::clone(&result_fn),
                Rc::clone(&yield_fn),
            );
            let mut todo2 = Work::<Tr1::Cursor, <Tr2::Batch as BatchReader>::Cursor, _, _, _>::new(
                result_fn, yield_fn,
            );

            // We'll unload the initial batches here, to put ourselves in a less non-deterministic state to start.
            trace1.map_batches(|batch1| {
                trace!(
                    operator_id,
                    input = 1,
                    lower = ?batch1.lower().elements(),
                    upper = ?batch1.upper().elements(),
                    size = batch1.len(),
                    "pre-loading batch",
                );

                acknowledged1.clone_from(batch1.upper());
                // No `todo1` work here, because we haven't accepted anything into `batches2` yet.
                // It is effectively "empty", because we choose to drain `trace1` before `trace2`.
                // Once we start streaming batches in, we will need to respond to new batches from
                // `input1` with logic that would have otherwise been here. Check out the next loop
                // for the structure.
            });
            // At this point, `ack1` should exactly equal `trace1.read_upper()`, as they are both determined by
            // iterating through batches and capturing the upper bound. This is a great moment to assert that
            // `trace1`'s physical compaction frontier is before the frontier of completed times in `trace1`.
            // TODO: in the case that this does not hold, instead "upgrade" the physical compaction frontier.
            assert!(PartialOrder::less_equal(
                &trace1.get_physical_compaction(),
                &acknowledged1.borrow()
            ));

            trace!(
                operator_id,
                input = 1,
                acknowledged1 = ?acknowledged1.elements(),
                "pre-loading finished",
            );

            // We capture batch2 cursors first and establish work second to avoid taking a `RefCell` lock
            // on both traces at the same time, as they could be the same trace and this would panic.
            let mut batch2_cursors = Vec::new();
            trace2.map_batches(|batch2| {
                trace!(
                    operator_id,
                    input = 2,
                    lower = ?batch2.lower().elements(),
                    upper = ?batch2.upper().elements(),
                    size = batch2.len(),
                    "pre-loading batch",
                );

                acknowledged2.clone_from(batch2.upper());
                batch2_cursors.push((batch2.cursor(), batch2.clone()));
            });
            // At this point, `ack2` should exactly equal `trace2.read_upper()`, as they are both determined by
            // iterating through batches and capturing the upper bound. This is a great moment to assert that
            // `trace2`'s physical compaction frontier is before the frontier of completed times in `trace2`.
            // TODO: in the case that this does not hold, instead "upgrade" the physical compaction frontier.
            assert!(PartialOrder::less_equal(
                &trace2.get_physical_compaction(),
                &acknowledged2.borrow()
            ));

            // Load up deferred work using trace2 cursors and batches captured just above.
            for (batch2_cursor, batch2) in batch2_cursors.into_iter() {
                trace!(
                    operator_id,
                    input = 2,
                    acknowledged1 = ?acknowledged1.elements(),
                    "deferring work for batch",
                );

                // It is safe to ask for `ack1` because we have confirmed it to be in advance of `distinguish_since`.
                let (trace1_cursor, trace1_storage) =
                    trace1.cursor_through(acknowledged1.borrow()).unwrap();
                // We could downgrade the capability here, but doing so is a bit complicated mathematically.
                // TODO: downgrade the capability by searching out the one time in `batch2.lower()` and not
                // in `batch2.upper()`. Only necessary for non-empty batches, as empty batches may not have
                // that property.
                todo2.push(
                    trace1_cursor,
                    trace1_storage,
                    batch2_cursor,
                    batch2.clone(),
                    capability.clone(),
                );
            }

            trace!(
                operator_id,
                input = 2,
                acknowledged2 = ?acknowledged2.elements(),
                "pre-loading finished",
            );

            // Droppable handles to shared trace data structures.
            let mut trace1_option = Some(trace1);
            let mut trace2_option = Some(trace2);

            move |input1, input2, output| {
                // If the dataflow is shutting down, discard all existing and future work.
                if shutdown_probe.in_shutdown() {
                    trace!(operator_id, "shutting down");

                    // Discard data at the inputs.
                    input1.for_each(|_cap, _data| ());
                    input2.for_each(|_cap, _data| ());

                    // Discard queued work.
                    todo1.discard();
                    todo2.discard();

                    // Stop holding on to input traces.
                    trace1_option = None;
                    trace2_option = None;

                    return;
                }

                // 1. Consuming input.
                //
                // The join computation repeatedly accepts batches of updates from each of its inputs.
                //
                // For each accepted batch, it prepares a work-item to join the batch against previously "accepted"
                // updates from its other input. It is important to track which updates have been accepted, because
                // we use a shared trace and there may be updates present that are in advance of this accepted bound.
                //
                // Batches are accepted: 1. in bulk at start-up (above), 2. as we observe them in the input stream,
                // and 3. if the trace can confirm a region of empty space directly following our accepted bound.
                // This last case is a consequence of our inability to transmit empty batches, as they may be formed
                // in the absence of timely dataflow capabilities.

                // Drain input 1, prepare work.
                input1.for_each(|capability, data| {
                    let trace2 = trace2_option
                        .as_mut()
                        .expect("we only drop a trace in response to the other input emptying");
                    let capability = capability.retain();
                    for batch1 in data.drain(..) {
                        // Ignore any pre-loaded data.
                        if PartialOrder::less_equal(&acknowledged1, batch1.lower()) {
                            trace!(
                                operator_id,
                                input = 1,
                                lower = ?batch1.lower().elements(),
                                upper = ?batch1.upper().elements(),
                                size = batch1.len(),
                                "loading batch",
                            );

                            if !batch1.is_empty() {
                                trace!(
                                    operator_id,
                                    input = 1,
                                    acknowledged2 = ?acknowledged2.elements(),
                                    "deferring work for batch",
                                );

                                // It is safe to ask for `ack2` as we validated that it was at least `get_physical_compaction()`
                                // at start-up, and have held back physical compaction ever since.
                                let (trace2_cursor, trace2_storage) =
                                    trace2.cursor_through(acknowledged2.borrow()).unwrap();
                                let batch1_cursor = batch1.cursor();
                                todo1.push(
                                    batch1_cursor,
                                    batch1.clone(),
                                    trace2_cursor,
                                    trace2_storage,
                                    capability.clone(),
                                );
                            }

                            // To update `acknowledged1` we might presume that `batch1.lower` should equal it, but we
                            // may have skipped over empty batches. Still, the batches are in-order, and we should be
                            // able to just assume the most recent `batch1.upper`
                            debug_assert!(PartialOrder::less_equal(&acknowledged1, batch1.upper()));
                            acknowledged1.clone_from(batch1.upper());

                            trace!(
                                operator_id,
                                input = 1,
                                acknowledged1 = ?acknowledged1.elements(),
                                "batch acknowledged",
                            );
                        }
                    }
                });

                // Drain input 2, prepare work.
                input2.for_each(|capability, data| {
                    let trace1 = trace1_option
                        .as_mut()
                        .expect("we only drop a trace in response to the other input emptying");
                    let capability = capability.retain();
                    for batch2 in data.drain(..) {
                        // Ignore any pre-loaded data.
                        if PartialOrder::less_equal(&acknowledged2, batch2.lower()) {
                            trace!(
                                operator_id,
                                input = 2,
                                lower = ?batch2.lower().elements(),
                                upper = ?batch2.upper().elements(),
                                size = batch2.len(),
                                "loading batch",
                            );

                            if !batch2.is_empty() {
                                trace!(
                                    operator_id,
                                    input = 2,
                                    acknowledged1 = ?acknowledged1.elements(),
                                    "deferring work for batch",
                                );

                                // It is safe to ask for `ack1` as we validated that it was at least `get_physical_compaction()`
                                // at start-up, and have held back physical compaction ever since.
                                let (trace1_cursor, trace1_storage) =
                                    trace1.cursor_through(acknowledged1.borrow()).unwrap();
                                let batch2_cursor = batch2.cursor();
                                todo2.push(
                                    trace1_cursor,
                                    trace1_storage,
                                    batch2_cursor,
                                    batch2.clone(),
                                    capability.clone(),
                                );
                            }

                            // To update `acknowledged2` we might presume that `batch2.lower` should equal it, but we
                            // may have skipped over empty batches. Still, the batches are in-order, and we should be
                            // able to just assume the most recent `batch2.upper`
                            debug_assert!(PartialOrder::less_equal(&acknowledged2, batch2.upper()));
                            acknowledged2.clone_from(batch2.upper());

                            trace!(
                                operator_id,
                                input = 2,
                                acknowledged2 = ?acknowledged2.elements(),
                                "batch acknowledged",
                            );
                        }
                    }
                });

                // Advance acknowledged frontiers through any empty regions that we may not receive as batches.
                if let Some(trace1) = trace1_option.as_mut() {
                    trace!(
                        operator_id,
                        input = 1,
                        acknowledged1 = ?acknowledged1.elements(),
                        "advancing trace upper",
                    );
                    trace1.advance_upper(&mut acknowledged1);
                }
                if let Some(trace2) = trace2_option.as_mut() {
                    trace!(
                        operator_id,
                        input = 2,
                        acknowledged2 = ?acknowledged2.elements(),
                        "advancing trace upper",
                    );
                    trace2.advance_upper(&mut acknowledged2);
                }

                // 2. Join computation.
                //
                // For each of the inputs, we do some amount of work (measured in terms of number
                // of output records produced). This is meant to yield control to allow downstream
                // operators to consume and reduce the output, but it it also means to provide some
                // degree of responsiveness. There is a potential risk here that if we fall behind
                // then the increasing queues hold back physical compaction of the underlying traces
                // which results in unintentionally quadratic processing time (each batch of either
                // input must scan all batches from the other input).

                // Perform some amount of outstanding work for input 1.
                trace!(
                    operator_id,
                    input = 1,
                    work_left = todo1.remaining(),
                    "starting work"
                );
                todo1.process(output);
                trace!(
                    operator_id,
                    input = 1,
                    work_left = todo1.remaining(),
                    "ceasing work",
                );

                // Perform some amount of outstanding work for input 2.
                trace!(
                    operator_id,
                    input = 2,
                    work_left = todo2.remaining(),
                    "starting work"
                );
                todo2.process(output);
                trace!(
                    operator_id,
                    input = 2,
                    work_left = todo2.remaining(),
                    "ceasing work",
                );

                // Re-activate operator if work remains.
                if !todo1.is_empty() || !todo2.is_empty() {
                    activator.activate();
                }

                // 3. Trace maintenance.
                //
                // Importantly, we use `input.frontier()` here rather than `acknowledged` to track
                // the progress of an input, because should we ever drop one of the traces we will
                // lose the ability to extract information from anything other than the input.
                // For example, if we dropped `trace2` we would not be able to use `advance_upper`
                // to keep `acknowledged2` up to date wrt empty batches, and would hold back logical
                // compaction of `trace1`.

                // Maintain `trace1`. Drop if `input2` is empty, or advance based on future needs.
                if let Some(trace1) = trace1_option.as_mut() {
                    if input2.frontier().is_empty() {
                        trace!(operator_id, input = 1, "dropping trace handle");
                        trace1_option = None;
                    } else {
                        trace!(
                            operator_id,
                            input = 1,
                            logical = ?*input2.frontier().frontier(),
                            physical = ?acknowledged1.elements(),
                            "advancing trace compaction",
                        );

                        // Allow `trace1` to compact logically up to the frontier we may yet receive,
                        // in the opposing input (`input2`). All `input2` times will be beyond this
                        // frontier, and joined times only need to be accurate when advanced to it.
                        trace1.set_logical_compaction(input2.frontier().frontier());
                        // Allow `trace1` to compact physically up to the upper bound of batches we
                        // have received in its input (`input1`). We will not require a cursor that
                        // is not beyond this bound.
                        trace1.set_physical_compaction(acknowledged1.borrow());
                    }
                }

                // Maintain `trace2`. Drop if `input1` is empty, or advance based on future needs.
                if let Some(trace2) = trace2_option.as_mut() {
                    if input1.frontier().is_empty() {
                        trace!(operator_id, input = 2, "dropping trace handle");
                        trace2_option = None;
                    } else {
                        trace!(
                            operator_id,
                            input = 2,
                            logical = ?*input1.frontier().frontier(),
                            physical = ?acknowledged2.elements(),
                            "advancing trace compaction",
                        );

                        // Allow `trace2` to compact logically up to the frontier we may yet receive,
                        // in the opposing input (`input1`). All `input1` times will be beyond this
                        // frontier, and joined times only need to be accurate when advanced to it.
                        trace2.set_logical_compaction(input1.frontier().frontier());
                        // Allow `trace2` to compact physically up to the upper bound of batches we
                        // have received in its input (`input2`). We will not require a cursor that
                        // is not beyond this bound.
                        trace2.set_physical_compaction(acknowledged2.borrow());
                    }
                }
            }
        },
    )
}

/// Work collected by the join operator.
///
/// The join operator enqueues new work here first, and then processes it at a controlled rate,
/// potentially yielding control to the Timely runtime in between. This allows it to avoid OOMs,
/// caused by buffering massive amounts of data at the output, and loss of interactivity.
///
/// Collected work can be reduced by calling the `process` method. The amount of work processed in
/// one go is controlled by the `yield_fn`.
struct Work<C1, C2, D, L, YFn>
where
    C1: Cursor,
    C2: Cursor,
{
    /// Pending work.
    todo: VecDeque<(Pin<Box<dyn Future<Output = ()>>>, Capability<C1::Time>)>,
    /// A function that transforms raw join matches into join results.
    result_fn: Rc<RefCell<L>>,
    /// A function that given a start time and an amount of results produced decides whether it is
    /// time to yield control.
    yield_fn: Rc<YFn>,
    /// Information shared with the work futures.
    shared: Rc<RefCell<Shared<D, C1::Time>>>,

    _cursors: PhantomData<(C1, C2)>,
}

impl<C1, C2, D, L, I, YFn> Work<C1, C2, D, L, YFn>
where
    C1: Cursor<Diff = Diff> + 'static,
    C2: for<'a> Cursor<Key<'a> = C1::Key<'a>, Time = C1::Time, Diff = Diff> + 'static,
    D: Data,
    L: FnMut(C1::Key<'_>, C1::Val<'_>, C2::Val<'_>) -> I + 'static,
    I: IntoIterator<Item = D> + 'static,
    YFn: Fn(Instant, usize) -> bool + 'static,
{
    fn new(result_fn: Rc<RefCell<L>>, yield_fn: Rc<YFn>) -> Self {
        Self {
            todo: Default::default(),
            result_fn,
            yield_fn,
            shared: Rc::new(RefCell::new(Shared {
                output: Default::default(),
                start_time: Instant::now(),
                produced: Default::default(),
            })),
            _cursors: PhantomData,
        }
    }

    /// Return the amount of remaining work chunks.
    fn remaining(&self) -> usize {
        self.todo.len()
    }

    /// Return whether there is any work pending.
    fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    /// Append some pending work.
    fn push(
        &mut self,
        cursor1: C1,
        storage1: C1::Storage,
        cursor2: C2,
        storage2: C2::Storage,
        capability: Capability<C1::Time>,
    ) {
        let joiner = Joiner {
            result_fn: Rc::clone(&self.result_fn),
            yield_fn: Rc::clone(&self.yield_fn),
            shared: Rc::clone(&self.shared),
            _cursors: PhantomData,
        };
        let fut = joiner.work(
            cursor1,
            storage1,
            cursor2,
            storage2,
            capability.time().clone(),
        );

        self.todo.push_back((Box::pin(fut), capability));
    }

    /// Discard all pending work.
    fn discard(&mut self) {
        self.todo = Default::default();
    }

    /// Process pending work until none is remaining or `yield_fn` requests a yield.
    fn process<C>(
        &mut self,
        output: &mut OutputHandleCore<C1::Time, CapacityContainerBuilder<C>, Tee<C1::Time, C>>,
    ) where
        C: SizableContainer + PushInto<(D, C1::Time, Diff)> + Data,
    {
        // Reset the `yield_fn` parameters.
        {
            let mut shared = self.shared.borrow_mut();
            shared.start_time = Instant::now();
            shared.produced = 0;
        }

        let waker = futures::task::noop_waker();
        let mut ctx = std::task::Context::from_waker(&waker);

        while let Some((mut fut, cap)) = self.todo.pop_front() {
            let done = fut.as_mut().poll(&mut ctx).is_ready();

            let mut shared = self.shared.borrow_mut();

            // Output the produced join results.
            // Consolidating here is important when the join closure produces data that
            // consolidates well, for example when projecting columns.
            let old_len = shared.output.len();
            consolidate_updates(&mut shared.output);
            shared.produced -= old_len - shared.output.len();

            output.session(&cap).give_iterator(shared.output.drain(..));

            if done {
                // We have finished processing a chunk of work. Use this opportunity to truncate
                // the output buffer, so we don't keep excess memory allocated forever.
                shared.output = Default::default();
            } else if !done {
                // Still work to do in this chunk.
                self.todo.push_front((fut, cap));
            }

            if (self.yield_fn)(shared.start_time, shared.produced) {
                break;
            }
        }
    }
}

/// Data shared between `Work::process` and the work futures.
struct Shared<D, T> {
    /// A buffer holding the join results.
    ///
    /// Written by the work futures, drained by `Work::process`.
    output: Vec<(D, T, Diff)>,
    /// The start time of the current `Work::process` invocation, for use with `yield_fn`.
    start_time: Instant,
    /// The numer of join results produced during the current `Work::process` invocation, for use
    /// with `yield_fn`.
    produced: usize,
}

struct Joiner<C1, C2, D, L, YFn>
where
    C1: Cursor,
    C2: Cursor,
{
    /// A function that transforms raw join matches into join results.
    result_fn: Rc<RefCell<L>>,
    /// A function that given a start time and an amount of results produced decides whether it is
    /// time to yield control.
    yield_fn: Rc<YFn>,
    /// Information shared with the work futures.
    shared: Rc<RefCell<Shared<D, C1::Time>>>,

    _cursors: PhantomData<(C1, C2)>,
}

impl<C1, C2, D, L, I, YFn> Joiner<C1, C2, D, L, YFn>
where
    C1: Cursor<Diff = Diff>,
    C2: for<'a> Cursor<Key<'a> = C1::Key<'a>, Time = C1::Time, Diff = Diff>,
    D: Data,
    L: FnMut(C1::Key<'_>, C1::Val<'_>, C2::Val<'_>) -> I + 'static,
    I: IntoIterator<Item = D> + 'static,
    YFn: Fn(Instant, usize) -> bool + 'static,
{
    /// Join the given cursors.
    ///
    /// This being implemented as an async function allows the work to be paused at any point
    /// `yield_fn` requests it. We are allowed to hold references across yield points, which is
    /// something we wouldn't get with a hand-rolled state machine.
    async fn work(
        self,
        mut cursor1: C1,
        storage1: C1::Storage,
        mut cursor2: C2,
        storage2: C2::Storage,
        meet: C1::Time,
    ) {
        let mut history1 = ValueHistory::new();
        let mut history2 = ValueHistory::new();

        while let Some(key1) = cursor1.get_key(&storage1)
            && let Some(key2) = cursor2.get_key(&storage2)
        {
            match key1.cmp(&key2) {
                Ordering::Less => cursor1.seek_key(&storage1, key2),
                Ordering::Greater => cursor2.seek_key(&storage2, key1),
                Ordering::Equal => {
                    history1.edits.load(&mut cursor1, &storage1, &meet);
                    history2.edits.load(&mut cursor2, &storage2, &meet);

                    self.join_key(key1, &mut history1, &mut history2).await;

                    cursor1.step_key(&storage1);
                    cursor2.step_key(&storage2);
                }
            }
        }
    }

    async fn join_key(
        &self,
        key: C1::Key<'_>,
        history1: &mut ValueHistory<'_, C1>,
        history2: &mut ValueHistory<'_, C2>,
    ) {
        if history1.edits.len() < 10 || history2.edits.len() < 10 {
            self.join_key_nested_loop(key, &history1.edits, &history2.edits);
        } else {
            self.join_key_linear_scan(key, history1, history2).await;
        }
    }

    fn join_key_nested_loop(
        &self,
        key: C1::Key<'_>,
        edits1: &EditList<'_, C1>,
        edits2: &EditList<'_, C2>,
    ) {
        let mut result_fn = self.result_fn.borrow_mut();
        let mut shared = self.shared.borrow_mut();

        edits1.for_each(|v1, t1, r1| {
            edits2.for_each(|v2, t2, r2| {
                for data in result_fn(key, v1, v2) {
                    shared.output.push((data, t1.join(t2), r1 * r2));
                    shared.produced += 1;
                }
            })
        })
    }

    async fn join_key_linear_scan(
        &self,
        key: C1::Key<'_>,
        history1: &mut ValueHistory<'_, C1>,
        history2: &mut ValueHistory<'_, C2>,
    ) {
        history1.replay();
        history2.replay();

        // TODO: It seems like there is probably a good deal of redundant `advance_buffer_by`
        //       in here. If a time is ever repeated, for example, the call will be identical
        //       and accomplish nothing. If only a single record has been added, it may not
        //       be worth the time to collapse (advance, re-sort) the data when a linear scan
        //       is sufficient.

        // Join the next entry in `history1`, return whether we should yield.
        let work_history1 = |history1: &mut ValueHistory<C1>, history2: &mut ValueHistory<C2>| {
            let mut result_fn = self.result_fn.borrow_mut();
            let mut shared = self.shared.borrow_mut();

            let (t1, meet, v1, r1) = history1.get().unwrap();
            history2.advance_past_by(meet);
            for &(v2, ref t2, r2) in &history2.past {
                for data in result_fn(key, v1, v2) {
                    shared.output.push((data, t1.join(t2), r1 * r2));
                    shared.produced += 1;
                }
            }
            history1.step();

            (self.yield_fn)(shared.start_time, shared.produced)
        };

        // Join the next entry in `history2`, return whether we should yield.
        let work_history2 = |history1: &mut ValueHistory<C1>, history2: &mut ValueHistory<C2>| {
            let mut result_fn = self.result_fn.borrow_mut();
            let mut shared = self.shared.borrow_mut();

            let (t2, meet, v2, r2) = history2.get().unwrap();
            history1.advance_past_by(meet);
            for &(v1, ref t1, r1) in &history1.past {
                for data in result_fn(key, v1, v2) {
                    shared.output.push((data, t1.join(t2), r1 * r2));
                    shared.produced += 1;
                }
            }
            history2.step();

            (self.yield_fn)(shared.start_time, shared.produced)
        };

        while let Some(time1) = history1.get_time()
            && let Some(time2) = history2.get_time()
        {
            let should_yield = if time1 < time2 {
                work_history1(history1, history2)
            } else {
                work_history2(history1, history2)
            };
            if should_yield {
                yield_now().await;
            }
        }

        while !history1.is_empty() {
            if work_history1(history1, history2) {
                yield_now().await;
            }
        }
        while !history2.is_empty() {
            if work_history2(history1, history2) {
                yield_now().await;
            }
        }
    }
}

/// An accumulation of (value, time, diff) updates.
struct EditList<'a, C: Cursor> {
    values: Vec<(C::Val<'a>, usize)>,
    edits: Vec<(C::Time, Diff)>,
}

impl<'a, C> EditList<'a, C>
where
    C: Cursor<Diff = Diff>,
{
    fn len(&self) -> usize {
        self.edits.len()
    }

    fn load(&mut self, cursor: &mut C, storage: &'a C::Storage, meet: &C::Time) {
        self.values.clear();
        self.edits.clear();

        let mut edit_idx = 0;
        while let Some(value) = cursor.get_val(storage) {
            cursor.map_times(storage, |time, diff| {
                let mut time = time.into_owned();
                time.join_assign(meet);
                self.edits.push((time, diff.into_owned()));
            });

            consolidate_from(&mut self.edits, edit_idx);

            if self.edits.len() > edit_idx {
                edit_idx = self.edits.len();
                self.values.push((value, edit_idx));
            }

            cursor.step_val(storage);
        }
    }

    fn for_each(&self, mut f: impl FnMut(C::Val<'a>, &C::Time, Diff)) {
        for (idx, (value, end)) in self.values.iter().enumerate() {
            let start = if idx == 0 { 0 } else { self.values[idx - 1].1 };
            for edit_idx in start..*end {
                let (time, diff) = &self.edits[edit_idx];
                f(*value, time, *diff);
            }
        }
    }
}

struct ValueHistory<'a, C: Cursor> {
    edits: EditList<'a, C>,
    future: Vec<(C::Time, C::Time, usize, Diff)>,
    past: Vec<(C::Val<'a>, C::Time, Diff)>,
}

impl<'a, C> ValueHistory<'a, C>
where
    C: Cursor,
{
    fn new() -> Self {
        Self {
            edits: EditList {
                values: Default::default(),
                edits: Default::default(),
            },
            future: Default::default(),
            past: Default::default(),
        }
    }

    fn is_empty(&self) -> bool {
        self.future.is_empty()
    }

    fn get(&self) -> Option<(&C::Time, &C::Time, C::Val<'a>, Diff)> {
        self.future.last().map(|(t, m, v, r)| {
            let (value, _) = self.edits.values[*v];
            (t, m, value, *r)
        })
    }

    fn get_time(&self) -> Option<&C::Time> {
        self.future.last().map(|(t, _, _, _)| t)
    }

    fn replay(&mut self) {
        self.future.clear();
        self.past.clear();

        let values = &self.edits.values;
        let edits = &self.edits.edits;
        for (idx, (_, end)) in values.iter().enumerate() {
            let start = if idx == 0 { 0 } else { values[idx - 1].1 };
            for edit_idx in start..*end {
                let (time, diff) = &edits[edit_idx];
                self.future.push((time.clone(), time.clone(), idx, *diff));
            }
        }

        self.future.sort_by(|x, y| y.cmp(x));

        for idx in 1..self.future.len() {
            self.future[idx].1 = self.future[idx].1.meet(&self.future[idx - 1].1);
        }
    }

    fn step(&mut self) {
        let (time, _, value_idx, diff) = self.future.pop().unwrap();
        let (value, _) = self.edits.values[value_idx];
        self.past.push((value, time, diff));
    }

    fn advance_past_by(&mut self, meet: &C::Time) {
        for (_, time, _) in &mut self.past {
            time.join_assign(meet);
        }
        consolidate_updates(&mut self.past);
    }
}
