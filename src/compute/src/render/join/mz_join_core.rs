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
//! `mz_join_core` resolves the loss-of-interactivity issue by also yielding within keys.
//!
//! For the moment, we keep both implementations around, selectable through feature flags.
//! Eventually, we hope that `mz_join_core` proves itself sufficiently to become the only join
//! implementation.

use std::cell::Cell;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::pin::Pin;
use std::rc::Rc;
use std::time::Instant;

use differential_dataflow::Data;
use differential_dataflow::consolidation::{consolidate_from, consolidate_updates};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::arrangement::Arranged;
use differential_dataflow::trace::{BatchReader, Cursor, TraceReader};
use mz_ore::future::yield_now;
use mz_repr::Diff;
use timely::container::{CapacityContainerBuilder, PushInto, SizableContainer};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::generic::OutputHandleCore;
use timely::dataflow::operators::{Capability, Operator};
use timely::dataflow::{Scope, StreamCore};
use timely::progress::timestamp::Timestamp;
use timely::{Container, PartialOrder};
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
    C: Container + SizableContainer + PushInto<(I::Item, G::Timestamp, Diff)> + Data,
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
            let mut todo1 = Work::<<Tr1::Batch as BatchReader>::Cursor, Tr2::Cursor, _, _>::new(
                Rc::clone(&result_fn),
            );
            let mut todo2 =
                Work::<Tr1::Cursor, <Tr2::Batch as BatchReader>::Cursor, _, _>::new(result_fn);

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
                todo1.process(output, &yield_fn);
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
                todo2.process(output, &yield_fn);
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
/// Collected work can be reduced by calling the `process` method.
struct Work<C1, C2, D, L>
where
    C1: Cursor,
    C2: Cursor,
{
    /// Pending work.
    todo: VecDeque<(Pin<Box<dyn Future<Output = ()>>>, Capability<C1::Time>)>,
    /// A function that transforms raw join matches into join results.
    result_fn: Rc<RefCell<L>>,
    /// A buffer holding the join results.
    ///
    /// Written by the work futures, drained by `Work::process`.
    output: Rc<RefCell<Vec<(D, C1::Time, Diff)>>>,
    /// The number of join results produced by work futures.
    ///
    /// Used with `yield_fn` to inform when `Work::process` should yield.
    produced: Rc<Cell<usize>>,

    _cursors: PhantomData<(C1, C2)>,
}

impl<C1, C2, D, L, I> Work<C1, C2, D, L>
where
    C1: Cursor<Diff = Diff> + 'static,
    C2: for<'a> Cursor<Key<'a> = C1::Key<'a>, Time = C1::Time, Diff = Diff> + 'static,
    D: Data,
    L: FnMut(C1::Key<'_>, C1::Val<'_>, C2::Val<'_>) -> I + 'static,
    I: IntoIterator<Item = D> + 'static,
{
    fn new(result_fn: Rc<RefCell<L>>) -> Self {
        Self {
            todo: Default::default(),
            result_fn,
            output: Default::default(),
            produced: Default::default(),
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
        let fut = self.start_work(
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
    fn process<C, YFn>(
        &mut self,
        output: &mut OutputHandleCore<C1::Time, CapacityContainerBuilder<C>, Tee<C1::Time, C>>,
        yield_fn: YFn,
    ) where
        C: Container + SizableContainer + PushInto<(D, C1::Time, Diff)> + Data,
        YFn: Fn(Instant, usize) -> bool,
    {
        let start_time = Instant::now();
        self.produced.set(0);

        let waker = futures::task::noop_waker();
        let mut ctx = std::task::Context::from_waker(&waker);

        while let Some((mut fut, cap)) = self.todo.pop_front() {
            // Drive the work future until it's done or it's time to yield.
            let mut done = false;
            let mut should_yield = false;
            while !done && !should_yield {
                done = fut.as_mut().poll(&mut ctx).is_ready();
                should_yield = yield_fn(start_time, self.produced.get());
            }

            // Drain the produced join results.
            let mut output_buf = self.output.borrow_mut();

            // Consolidating here is important when the join closure produces data that
            // consolidates well, for example when projecting columns.
            let old_len = output_buf.len();
            consolidate_updates(&mut output_buf);
            let recovered = old_len - output_buf.len();
            self.produced.update(|x| x - recovered);

            output.session(&cap).give_iterator(output_buf.drain(..));

            if done {
                // We have finished processing a chunk of work. Use this opportunity to truncate
                // the output buffer, so we don't keep excess memory allocated forever.
                *output_buf = Default::default();
            } else if !done {
                // Still work to do in this chunk.
                self.todo.push_front((fut, cap));
            }

            if should_yield {
                break;
            }
        }
    }

    /// Start the work of joining the updates produced by the given cursors.
    ///
    /// This method returns a `Future` that can be polled to make progress on the join work.
    /// Returning a future allows us to implement the logic using async/await syntax where we can
    /// conveniently pause the work at any point by calling `yield_now().await`. We are allowed to
    /// hold references across yield points, which is something we wouldn't get with a hand-rolled
    /// state machine implementation.
    fn start_work(
        &self,
        mut cursor1: C1,
        storage1: C1::Storage,
        mut cursor2: C2,
        storage2: C2::Storage,
        meet: C1::Time,
    ) -> impl Future<Output = ()> + use<C1, C2, D, L, I> {
        let result_fn = Rc::clone(&self.result_fn);
        let output = Rc::clone(&self.output);
        let produced = Rc::clone(&self.produced);

        async move {
            let mut joiner = Joiner::new(result_fn, output, produced, meet);

            while let Some(key1) = cursor1.get_key(&storage1)
                && let Some(key2) = cursor2.get_key(&storage2)
            {
                match key1.cmp(&key2) {
                    Ordering::Less => cursor1.seek_key(&storage1, key2),
                    Ordering::Greater => cursor2.seek_key(&storage2, key1),
                    Ordering::Equal => {
                        joiner
                            .join_key(key1, &mut cursor1, &storage1, &mut cursor2, &storage2)
                            .await;

                        cursor1.step_key(&storage1);
                        cursor2.step_key(&storage2);
                    }
                }
            }
        }
    }
}

/// Type that knows how to perform the core join logic.
///
/// The joiner implements two join strategies:
///
///  * The "simple" strategy produces a match for each combination of (val1, time1, val2, time2)
///    found in the inputs. If there are multiple times in the input, it may produce matches for
///    times in which one of the values wasn't present. These matches cancel each other out, so the
///    result ends up correct.
///  * The "linear scan over times" strategy sorts the input data by time and then steps through
///    the input histories, producing matches for a pair of values only if both values where
///    present at the same time.
///
/// The linear scan strategy avoids redundant work and is much more efficient than the simple
/// strategy when many distinct times are present in the inputs. However, sorting the input data
/// incurs some overhead, so we still prefer the simple variant when the input data is small.
struct Joiner<'a, C1, C2, D, L>
where
    C1: Cursor,
    C2: Cursor,
{
    /// A function that transforms raw join matches into join results.
    result_fn: Rc<RefCell<L>>,
    /// A buffer holding the join results.
    output: Rc<RefCell<Vec<(D, C1::Time, Diff)>>>,
    /// The number of join results produced.
    produced: Rc<Cell<usize>>,
    /// A time to which all join results should be advanced.
    meet: C1::Time,

    /// Buffer for edit histories from the first input.
    history1: ValueHistory<'a, C1>,
    /// Buffer for edit histories from the second input.
    history2: ValueHistory<'a, C2>,
}

impl<'a, C1, C2, D, L, I> Joiner<'a, C1, C2, D, L>
where
    C1: Cursor<Diff = Diff>,
    C2: Cursor<Key<'a> = C1::Key<'a>, Time = C1::Time, Diff = Diff>,
    D: Data,
    L: FnMut(C1::Key<'_>, C1::Val<'_>, C2::Val<'_>) -> I + 'static,
    I: IntoIterator<Item = D> + 'static,
{
    fn new(
        result_fn: Rc<RefCell<L>>,
        output: Rc<RefCell<Vec<(D, C1::Time, Diff)>>>,
        produced: Rc<Cell<usize>>,
        meet: C1::Time,
    ) -> Self {
        Self {
            result_fn,
            output,
            produced,
            meet,
            history1: ValueHistory::new(),
            history2: ValueHistory::new(),
        }
    }

    /// Produce matches for the values of a single key.
    async fn join_key(
        &mut self,
        key: C1::Key<'_>,
        cursor1: &mut C1,
        storage1: &'a C1::Storage,
        cursor2: &mut C2,
        storage2: &'a C2::Storage,
    ) {
        self.history1.edits.load(cursor1, storage1, &self.meet);
        self.history2.edits.load(cursor2, storage2, &self.meet);

        // If the input data is small, use the simple strategy.
        //
        // TODO: This conditional is taken directly from DD. We should check if it might make sense
        //       to do something different, like using the simple strategy always when the number
        //       of distinct times is small.
        if self.history1.edits.len() < 10 || self.history2.edits.len() < 10 {
            self.join_key_simple(key);
            yield_now().await;
        } else {
            self.join_key_linear_time_scan(key).await;
        }
    }

    /// Produce matches for the values of a single key, using the simple strategy.
    ///
    /// This strategy is only meant to be used for small inputs, so we don't bother including yield
    /// points or optimizations.
    fn join_key_simple(&self, key: C1::Key<'_>) {
        let mut result_fn = self.result_fn.borrow_mut();
        let mut output = self.output.borrow_mut();

        for (v1, t1, r1) in self.history1.edits.iter() {
            for (v2, t2, r2) in self.history2.edits.iter() {
                for data in result_fn(key, v1, v2) {
                    output.push((data, t1.join(t2), r1 * r2));
                    self.produced.update(|x| x + 1);
                }
            }
        }
    }

    /// Produce matches for the values of a single key, using a linear scan through times.
    async fn join_key_linear_time_scan(&mut self, key: C1::Key<'_>) {
        let history1 = &mut self.history1;
        let history2 = &mut self.history2;

        history1.replay();
        history2.replay();

        // TODO: It seems like there is probably a good deal of redundant `advance_buffer_by`
        //       in here. If a time is ever repeated, for example, the call will be identical
        //       and accomplish nothing. If only a single record has been added, it may not
        //       be worth the time to collapse (advance, re-sort) the data when a linear scan
        //       is sufficient.

        // Join the next entry in `history1`.
        let work_history1 = |history1: &mut ValueHistory<C1>, history2: &mut ValueHistory<C2>| {
            let mut result_fn = self.result_fn.borrow_mut();
            let mut output = self.output.borrow_mut();

            let (t1, meet, v1, r1) = history1.get().unwrap();
            history2.advance_past_by(meet);
            for &(v2, ref t2, r2) in &history2.past {
                for data in result_fn(key, v1, v2) {
                    output.push((data, t1.join(t2), r1 * r2));
                    self.produced.update(|x| x + 1);
                }
            }
            history1.step();
        };

        // Join the next entry in `history2`.
        let work_history2 = |history1: &mut ValueHistory<C1>, history2: &mut ValueHistory<C2>| {
            let mut result_fn = self.result_fn.borrow_mut();
            let mut output = self.output.borrow_mut();

            let (t2, meet, v2, r2) = history2.get().unwrap();
            history1.advance_past_by(meet);
            for &(v1, ref t1, r1) in &history1.past {
                for data in result_fn(key, v1, v2) {
                    output.push((data, t1.join(t2), r1 * r2));
                    self.produced.update(|x| x + 1);
                }
            }
            history2.step();
        };

        while let Some(time1) = history1.get_time()
            && let Some(time2) = history2.get_time()
        {
            if time1 < time2 {
                work_history1(history1, history2)
            } else {
                work_history2(history1, history2)
            };
            yield_now().await;
        }

        while !history1.is_empty() {
            work_history1(history1, history2);
            yield_now().await;
        }
        while !history2.is_empty() {
            work_history2(history1, history2);
            yield_now().await;
        }
    }
}

/// An accumulation of (value, time, diff) updates.
///
/// Deduplicated values are stored in `values`. Each entry includes the end index of the
/// corresponding range in `edits`. The edits stored for a value are consolidated.
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

    /// Load the updates in the given cursor.
    ///
    /// Steps over values, but not over keys.
    fn load(&mut self, cursor: &mut C, storage: &'a C::Storage, meet: &C::Time) {
        self.values.clear();
        self.edits.clear();

        let mut edit_idx = 0;
        while let Some(value) = cursor.get_val(storage) {
            cursor.map_times(storage, |time, diff| {
                let mut time = C::owned_time(time);
                time.join_assign(meet);
                self.edits.push((time, C::owned_diff(diff)));
            });

            consolidate_from(&mut self.edits, edit_idx);

            if self.edits.len() > edit_idx {
                edit_idx = self.edits.len();
                self.values.push((value, edit_idx));
            }

            cursor.step_val(storage);
        }
    }

    /// Iterate over the contained updates.
    fn iter(&self) -> impl Iterator<Item = (C::Val<'a>, &C::Time, Diff)> {
        self.values
            .iter()
            .enumerate()
            .flat_map(|(idx, (value, end))| {
                let start = if idx == 0 { 0 } else { self.values[idx - 1].1 };
                let edits = &self.edits[start..*end];
                edits.iter().map(|(time, diff)| (*value, time, *diff))
            })
    }
}

/// A history for replaying updates in time order.
struct ValueHistory<'a, C: Cursor> {
    /// Unsorted updates to replay.
    edits: EditList<'a, C>,
    /// Time-sorted updates that have not been stepped over yet.
    ///
    /// Entries are (time, meet, value_idx, diff).
    future: Vec<(C::Time, C::Time, usize, Diff)>,
    /// Rolled-up updates that have been stepped over.
    past: Vec<(C::Val<'a>, C::Time, Diff)>,
}

impl<'a, C> ValueHistory<'a, C>
where
    C: Cursor,
{
    /// Create a new empty `ValueHistory`.
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

    /// Return whether there are updates left to step over.
    fn is_empty(&self) -> bool {
        self.future.is_empty()
    }

    /// Return the next update.
    fn get(&self) -> Option<(&C::Time, &C::Time, C::Val<'a>, Diff)> {
        self.future.last().map(|(t, m, v, r)| {
            let (value, _) = self.edits.values[*v];
            (t, m, value, *r)
        })
    }

    /// Return the time of the next update.
    fn get_time(&self) -> Option<&C::Time> {
        self.future.last().map(|(t, _, _, _)| t)
    }

    /// Populate `future` with the updates stored in `edits`.
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

    /// Advance the history by moving the next entry from `future` into `past`.
    fn step(&mut self) {
        let (time, _, value_idx, diff) = self.future.pop().unwrap();
        let (value, _) = self.edits.values[value_idx];
        self.past.push((value, time, diff));
    }

    /// Advance all times in `past` by `meet`.
    fn advance_past_by(&mut self, meet: &C::Time) {
        for (_, time, _) in &mut self.past {
            time.join_assign(meet);
        }
        consolidate_updates(&mut self.past);
    }
}
