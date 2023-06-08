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

use std::cell::{Cell, RefCell};
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::pin::Pin;
use std::rc::Rc;

use differential_dataflow::consolidation::{consolidate_from, consolidate_updates};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::arrangement::Arranged;
use differential_dataflow::trace::{BatchReader, Cursor, TraceReader};
use differential_dataflow::{AsCollection, Collection, Data};
use futures::Future;
use mz_repr::{Diff, Row};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::generic::OutputHandle;
use timely::dataflow::operators::{Capability, Operator};
use timely::dataflow::Scope;
use timely::progress::timestamp::Timestamp;
use timely::scheduling::Activator;
use timely::PartialOrder;
use tokio::task::yield_now;

/// Joins two arranged collections with the same key type.
///
/// Each matching pair of records `(key, val1)` and `(key, val2)` are subjected to the `result` function,
/// which produces something implementing `IntoIterator`, where the output collection will have an entry for
/// every value returned by the iterator.
pub(super) fn mz_join_core<G, Tr1, Tr2, L, I>(
    arranged1: &Arranged<G, Tr1>,
    arranged2: &Arranged<G, Tr2>,
    result: L,
) -> Collection<G, I::Item, Diff>
where
    G: Scope,
    G::Timestamp: Lattice,
    Tr1: TraceReader<Key = Row, Val = Row, Time = G::Timestamp, R = Diff> + Clone + 'static,
    Tr2: TraceReader<Key = Row, Val = Row, Time = G::Timestamp, R = Diff> + Clone + 'static,
    L: FnMut(&Tr1::Key, &Tr1::Val, &Tr2::Val) -> I + 'static,
    I: IntoIterator + 'static,
    I::Item: Data,
{
    let mut trace1 = arranged1.trace.clone();
    let mut trace2 = arranged2.trace.clone();

    arranged1
        .stream
        .binary_frontier(
            &arranged2.stream,
            Pipeline,
            Pipeline,
            "Join",
            move |capability, info| {
                // Acquire an activator to reschedule the operator when it has unfinished work.
                let activations = arranged1.stream.scope().activations();
                let activator = Activator::new(&info.address[..], activations);

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
                let mut todo1 = Work::new(Rc::clone(&result_fn));
                let mut todo2 = Work::new(Rc::clone(&result_fn));

                // We'll unload the initial batches here, to put ourselves in a less non-deterministic state to start.
                trace1.map_batches(|batch1| {
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

                // We capture batch2 cursors first and establish work second to avoid taking a `RefCell` lock
                // on both traces at the same time, as they could be the same trace and this would panic.
                let mut batch2_cursors = Vec::new();
                trace2.map_batches(|batch2| {
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

                // Droppable handles to shared trace data structures.
                let mut trace1_option = Some(trace1);
                let mut trace2_option = Some(trace2);

                // Swappable buffers for input extraction.
                let mut input1_buffer = Vec::new();
                let mut input2_buffer = Vec::new();

                move |input1, input2, output| {
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
                        data.swap(&mut input1_buffer);
                        for batch1 in input1_buffer.drain(..) {
                            // Ignore any pre-loaded data.
                            if PartialOrder::less_equal(&acknowledged1, batch1.lower()) {
                                if !batch1.is_empty() {
                                    // It is safe to ask for `ack2` as we validated that it was at least `get_physical_compaction()`
                                    // at start-up, and have held back physical compaction ever since.
                                    let (trace2_cursor, trace2_storage) =
                                        trace2.cursor_through(acknowledged2.borrow()).unwrap();
                                    todo1.push(
                                        batch1.cursor(),
                                        batch1.clone(),
                                        trace2_cursor,
                                        trace2_storage,
                                        capability.clone(),
                                    );
                                }

                                // To update `acknowledged1` we might presume that `batch1.lower` should equal it, but we
                                // may have skipped over empty batches. Still, the batches are in-order, and we should be
                                // able to just assume the most recent `batch1.upper`
                                debug_assert!(PartialOrder::less_equal(
                                    &acknowledged1,
                                    batch1.upper()
                                ));
                                acknowledged1.clone_from(batch1.upper());
                            }
                        }
                    });

                    // Drain input 2, prepare work.
                    input2.for_each(|capability, data| {
                        let trace1 = trace1_option
                            .as_mut()
                            .expect("we only drop a trace in response to the other input emptying");
                        let capability = capability.retain();
                        data.swap(&mut input2_buffer);
                        for batch2 in input2_buffer.drain(..) {
                            // Ignore any pre-loaded data.
                            if PartialOrder::less_equal(&acknowledged2, batch2.lower()) {
                                if !batch2.is_empty() {
                                    // It is safe to ask for `ack1` as we validated that it was at least `get_physical_compaction()`
                                    // at start-up, and have held back physical compaction ever since.
                                    let (trace1_cursor, trace1_storage) =
                                        trace1.cursor_through(acknowledged1.borrow()).unwrap();
                                    todo2.push(
                                        trace1_cursor,
                                        trace1_storage,
                                        batch2.cursor(),
                                        batch2.clone(),
                                        capability.clone(),
                                    );
                                }

                                // To update `acknowledged2` we might presume that `batch2.lower` should equal it, but we
                                // may have skipped over empty batches. Still, the batches are in-order, and we should be
                                // able to just assume the most recent `batch2.upper`
                                debug_assert!(PartialOrder::less_equal(
                                    &acknowledged2,
                                    batch2.upper()
                                ));
                                acknowledged2.clone_from(batch2.upper());
                            }
                        }
                    });

                    // Advance acknowledged frontiers through any empty regions that we may not receive as batches.
                    if let Some(trace1) = trace1_option.as_mut() {
                        trace1.advance_upper(&mut acknowledged1);
                    }
                    if let Some(trace2) = trace2_option.as_mut() {
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

                    // Perform some amount of outstanding work.
                    let fuel = 1_000_000;
                    todo1.process(output, fuel);
                    todo2.process(output, fuel);

                    // Re-activate operator if work remains.
                    if todo1.work_remaining() || todo2.work_remaining() {
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
                            trace1_option = None;
                        } else {
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
                            trace2_option = None;
                        } else {
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
        .as_collection()
}

/// Work collected by the join operator.
///
/// Work can be reduced by calling the `process` method. The amount of work processed in one go is
/// controlled by the amount of fuel provided. Once the given fuel is exhausted, `process` returns,
/// to allow the join operator to yield control back to Timely and thereby retain responsiveness of
/// the system.
///
/// Parts of the joining work operator on references, rather than `Cursor`s directly. If we would
/// store these directly we would end up with a self-referential struct, which safe Rust doesn't
/// support normally. An exception is the `async` keyword with lets us construct `Future`s which do
/// have the ability to store references to their own state. So we can wrap the joining work inside
/// an `in_progress` future and manually poll that. At any point throughout work processing (e.g.,
/// when the fuel is exhausted) we can yield back to the operator by calling `await` inside the
/// `async fn` that constructs the future.
struct Work<C1, C2, D, T, L>
where
    C1: Cursor<Key = Row, Val = Row, Time = T, R = Diff>,
    C2: Cursor<Key = Row, Val = Row, Time = T, R = Diff>,
    T: Timestamp,
{
    /// Work we have not yet started.
    backlog: VecDeque<Deferred<C1, C2, T>>,
    /// Work currently in progress, if any.
    in_progress: Option<(Pin<Box<dyn Future<Output = ()>>>, Capability<T>)>,

    // Stuff shared with the `in_progress` future:
    /// A function that transforms raw join matches into join results.
    result_fn: Rc<RefCell<L>>,
    /// A buffer holding the join results.
    /// Written by the `in_progress` future, collected by us.
    results: Rc<RefCell<Vec<(D, T, Diff)>>>,
    /// The fuel available for join processing.
    /// Consumed by the `in_progress` future, replenished by us.
    fuel: Rc<Cell<u64>>,
}

impl<C1, C2, D, T, L, I> Work<C1, C2, D, T, L>
where
    C1: Cursor<Key = Row, Val = Row, Time = T, R = Diff> + 'static,
    C2: Cursor<Key = Row, Val = Row, Time = T, R = Diff> + 'static,
    D: Data,
    T: Timestamp + Lattice,
    L: FnMut(&C1::Key, &C1::Val, &C2::Val) -> I + 'static,
    I: IntoIterator<Item = D> + 'static,
{
    fn new(result_fn: Rc<RefCell<L>>) -> Self {
        Self {
            backlog: Default::default(),
            in_progress: Default::default(),
            result_fn,
            results: Default::default(),
            fuel: Default::default(),
        }
    }

    fn work_remaining(&self) -> bool {
        self.in_progress.is_some() || !self.backlog.is_empty()
    }

    /// Append some pending work.
    fn push(
        &mut self,
        trace: C1,
        trace_storage: C1::Storage,
        batch: C2,
        batch_storage: C2::Storage,
        capability: Capability<T>,
    ) {
        self.backlog.push_back(Deferred {
            cursor1: trace,
            storage1: trace_storage,
            cursor2: batch,
            storage2: batch_storage,
            capability,
        });
    }

    /// Process pending work until none is remaining of the `fuel` is exhausted.
    fn process(
        &mut self,
        output: &mut OutputHandle<T, (D, T, Diff), Tee<T, (D, T, Diff)>>,
        fuel: u64,
    ) {
        // Replenish the fuel available to the `in_progress` future.
        self.fuel.set(fuel);

        while self.work_remaining() && self.fuel.get() > 0 {
            let (fut, cap) = match &mut self.in_progress {
                Some(progress) => progress,
                None => {
                    // Start new progress.
                    let deferred = self.backlog.pop_front().expect("work is remaining");
                    let cap = deferred.capability.clone();
                    let fut = deferred.work(
                        Rc::clone(&self.result_fn),
                        Rc::clone(&self.results),
                        Rc::clone(&self.fuel),
                    );
                    self.in_progress = Some((Box::pin(fut), cap));
                    self.in_progress.as_mut().unwrap()
                }
            };

            // Poll the future. When the `poll` returns the in-progress work has either finished or
            // the fuel was exhausted. In any case the `results` buffer will contain a chunk of
            // output that we can produce.
            //
            // Why use a `noop_waker` which ignores `wake` calls. We could wire up a waker so that
            // it activates the join operator again, but it is easier to just manually invoke an
            // `Activator` if work is remaining after we return.
            let waker = futures::task::noop_waker();
            let mut ctx = std::task::Context::from_waker(&waker);
            let done = fut.as_mut().poll(&mut ctx).is_ready();

            // Here we could instead `drain` the `results` buffer. `take`ing it instead ensures
            // that we don't keep excess memory around if earlier produced batches (e.g. from
            // processing a snapshot) are large but later ones are much smaller.
            let mut results = std::mem::take(&mut *self.results.borrow_mut());

            // TODO: This consolidation is optional, and it may not be very helpful. We might try
            //       harder to understand whether we should do this work here, or downstream at
            //       consumers.
            consolidate_updates(&mut results);

            output.session(cap).give_container(&mut results);

            if done {
                self.in_progress = None;
            }
        }
    }
}

/// A batch of pending join work.
struct Deferred<C1, C2, T>
where
    T: Timestamp,
    C1: Cursor<Key = Row, Val = Row, Time = T, R = Diff>,
    C2: Cursor<Key = Row, Val = Row, Time = T, R = Diff>,
{
    cursor1: C1,
    storage1: C1::Storage,
    cursor2: C2,
    storage2: C2::Storage,
    capability: Capability<T>,
}

impl<C1, C2, T> Deferred<C1, C2, T>
where
    T: Timestamp + Lattice,
    C1: Cursor<Key = Row, Val = Row, Time = T, R = Diff>,
    C2: Cursor<Key = Row, Val = Row, Time = T, R = Diff>,
{
    /// Start processing the pending join work.
    ///
    /// This is an `async fn` to allow us to pause the work at any point, specifically when the
    /// given `fuel` is exhausted. Fuel exhaustion is the only reason why this method yields.
    ///
    /// Join matches are transformed through `result_fn` and then pushed to the `results` buffer.
    async fn work<D, L, I>(
        mut self,
        result_fn: Rc<RefCell<L>>,
        results: Rc<RefCell<Vec<(D, T, Diff)>>>,
        fuel: Rc<Cell<u64>>,
    ) where
        D: Data,
        L: FnMut(&C1::Key, &C1::Val, &C2::Val) -> I + 'static,
        I: IntoIterator<Item = D>,
    {
        let meet = self.capability.time();

        let cursor1 = &mut self.cursor1;
        let storage1 = &self.storage1;
        let cursor2 = &mut self.cursor2;
        let storage2 = &self.storage2;

        while cursor1.key_valid(storage1) && cursor2.key_valid(storage2) {
            let key1 = cursor1.key(storage1);
            let key2 = cursor2.key(storage2);
            match key1.cmp(key2) {
                Ordering::Less => cursor1.seek_key(storage1, key2),
                Ordering::Greater => cursor2.seek_key(storage2, key1),
                Ordering::Equal => {
                    let history1 = ValueHistory::load(cursor1, storage1, meet);
                    let history2 = ValueHistory::load(cursor2, storage2, meet);
                    let mut joiner = ValueJoiner { history1, history2 };

                    joiner
                        .join(|val1, val2, time, diff| {
                            // Invoke `result_fn` to produce join results, push these to the
                            // `results` buffer, then subtract fuel and tell the `joiner` to yield
                            // if necessary.

                            let mut count = 0;
                            let updates = result_fn.borrow_mut()(key1, val1, val2)
                                .into_iter()
                                .map(|d| (d, time.clone(), diff))
                                .inspect(|_| count += 1);
                            results.borrow_mut().extend(updates);

                            let remaining_fuel = fuel.get().saturating_sub(count);
                            let must_yield = remaining_fuel == 0;
                            fuel.set(remaining_fuel);
                            must_yield
                        })
                        .await;

                    cursor1.step_key(storage1);
                    cursor2.step_key(storage2);
                }
            }
        }
    }
}

struct ValueJoiner<'a, T> {
    history1: ValueHistory<'a, T>,
    history2: ValueHistory<'a, T>,
}

impl<'a, T> ValueJoiner<'a, T>
where
    T: Timestamp + Lattice,
{
    async fn join<F>(&mut self, result_fn: F)
    where
        F: FnMut(&Row, &Row, T, Diff) -> bool,
    {
        if self.history1.edits.len() < 10 || self.history2.edits.len() < 10 {
            self.nested_loop_join(result_fn);
        } else {
            self.merge_join(result_fn).await;
        }
    }

    fn nested_loop_join<F>(&mut self, mut result_fn: F)
    where
        F: FnMut(&Row, &Row, T, Diff) -> bool,
    {
        self.history1.edits.map(|v1, t1, r1| {
            self.history2.edits.map(|v2, t2, r2| {
                let t = t1.join(t2);
                let r = r1 * r2;
                result_fn(v1, v2, t, r);
            })
        })
    }

    async fn merge_join<F>(&mut self, mut result_fn: F)
    where
        F: FnMut(&Row, &Row, T, Diff) -> bool,
    {
        let mut replay1 = self.history1.replay();
        let mut replay2 = self.history2.replay();

        let mut emit_result = move |v1: &Row, v2: &Row, t1: &T, t2: &T, r1: Diff, r2: Diff| {
            let t = t1.join(t2);
            let r = r1 * r2;
            let must_yield = result_fn(v1, v2, t, r);

            async move {
                if must_yield {
                    yield_now().await;
                }
            }
        };

        // TODO: It seems like there is probably a good deal of redundant `advance_buffer_by`
        //       in here. If a time is ever repeated, for example, the call will be identical
        //       and accomplish nothing. If only a single record has been added, it may not
        //       be worth the time to collapse (advance, re-sort) the data when a linear scan
        //       is sufficient.

        while !replay1.is_done() && !replay2.is_done() {
            if replay1.time().unwrap() < replay2.time().unwrap() {
                replay2.advance_buffer_by(replay1.meet().unwrap());
                for (v2, t2, r2) in replay2.buffer().iter() {
                    let (v1, t1, r1) = replay1.edit().unwrap();
                    emit_result(v1, v2, t1, t2, r1, *r2).await;
                }
                replay1.step();
            } else {
                replay1.advance_buffer_by(replay2.meet().unwrap());
                for (v1, t1, r1) in replay1.buffer().iter() {
                    let (v2, t2, r2) = replay2.edit().unwrap();
                    emit_result(v1, v2, t1, t2, *r1, r2).await;
                }
                replay2.step();
            }
        }

        while !replay1.is_done() {
            replay2.advance_buffer_by(replay1.meet().unwrap());
            for (v2, t2, r2) in replay2.buffer().iter() {
                let (v1, t1, r1) = replay1.edit().unwrap();
                emit_result(v1, v2, t1, t2, r1, *r2).await;
            }
            replay1.step();
        }
        while !replay2.is_done() {
            replay1.advance_buffer_by(replay2.meet().unwrap());
            for (v1, t1, r1) in replay1.buffer().iter() {
                let (v2, t2, r2) = replay2.edit().unwrap();
                emit_result(v1, v2, t1, t2, *r1, r2).await;
            }
            replay2.step();
        }
    }
}

struct ValueHistory<'storage, T> {
    edits: EditList<'storage, T>,
    history: Vec<(T, T, usize, usize)>, // (time, meet, value_index, edit_offset)
    buffer: Vec<(&'storage Row, T, Diff)>, // where we accumulate / collapse updates.
}

impl<'storage, T> ValueHistory<'storage, T>
where
    T: Timestamp + Lattice,
{
    fn load<C>(cursor: &mut C, storage: &'storage C::Storage, meet: &T) -> Self
    where
        C: Cursor<Key = Row, Val = Row, Time = T, R = Diff>,
    {
        let mut edits = EditList::new();
        edits.load(cursor, storage, |t| t.join(meet));

        Self {
            edits,
            history: Vec::new(),
            buffer: Vec::new(),
        }
    }

    /// Organizes history based on current contents of edits.
    fn replay<'history>(&'history mut self) -> HistoryReplay<'storage, 'history, T> {
        self.buffer.clear();
        self.history.clear();
        for value_index in 0..self.edits.values.len() {
            let lower = if value_index > 0 {
                self.edits.values[value_index - 1].1
            } else {
                0
            };
            let upper = self.edits.values[value_index].1;
            for edit_index in lower..upper {
                let time = self.edits.edits[edit_index].0.clone();
                self.history
                    .push((time.clone(), time.clone(), value_index, edit_index));
            }
        }

        self.history.sort_by(|x, y| y.cmp(x));
        for index in 1..self.history.len() {
            self.history[index].1 = self.history[index].1.meet(&self.history[index - 1].1);
        }

        HistoryReplay { replay: self }
    }
}

/// An accumulation of (value, time, diff) updates.
struct EditList<'a, T> {
    values: Vec<(&'a Row, usize)>,
    edits: Vec<(T, Diff)>,
}

impl<'a, T> EditList<'a, T>
where
    T: Timestamp,
{
    /// Creates an empty list of edits.
    fn new() -> Self {
        EditList {
            values: Vec::new(),
            edits: Vec::new(),
        }
    }

    /// Loads the contents of a cursor.
    fn load<C, L>(&mut self, cursor: &mut C, storage: &'a C::Storage, logic: L)
    where
        C: Cursor<Val = Row, Time = T, R = Diff>,
        C::Key: Eq,
        L: Fn(&T) -> T,
    {
        self.clear();
        while cursor.val_valid(storage) {
            cursor.map_times(storage, |time1, diff1| {
                self.push(logic(time1), diff1.clone())
            });
            self.seal(cursor.val(storage));
            cursor.step_val(storage);
        }
    }

    /// Clears the list of edits.
    fn clear(&mut self) {
        self.values.clear();
        self.edits.clear();
    }

    fn len(&self) -> usize {
        self.edits.len()
    }

    /// Inserts a new edit for an as-yet undetermined value.
    fn push(&mut self, time: T, diff: Diff) {
        // TODO: Could attempt "insertion-sort" like behavior here, where we collapse if possible.
        self.edits.push((time, diff));
    }

    /// Associates all edits pushed since the previous `seal_value` call with `value`.
    fn seal(&mut self, value: &'a Row) {
        let prev = self.values.last().map(|x| x.1).unwrap_or(0);
        consolidate_from(&mut self.edits, prev);
        if self.edits.len() > prev {
            self.values.push((value, self.edits.len()));
        }
    }

    fn map<F>(&self, mut logic: F)
    where
        F: FnMut(&Row, &T, Diff),
    {
        for index in 0..self.values.len() {
            let lower = if index == 0 {
                0
            } else {
                self.values[index - 1].1
            };
            let upper = self.values[index].1;
            for edit in lower..upper {
                logic(
                    self.values[index].0,
                    &self.edits[edit].0,
                    self.edits[edit].1.clone(),
                );
            }
        }
    }
}

struct HistoryReplay<'storage, 'history, T> {
    replay: &'history mut ValueHistory<'storage, T>,
}

impl<'storage, 'history, T> HistoryReplay<'storage, 'history, T>
where
    'storage: 'history,
    T: Timestamp + Lattice,
{
    fn time(&self) -> Option<&T> {
        self.replay.history.last().map(|x| &x.0)
    }

    fn meet(&self) -> Option<&T> {
        self.replay.history.last().map(|x| &x.1)
    }

    fn edit(&self) -> Option<(&Row, &T, Diff)> {
        self.replay.history.last().map(|&(ref t, _, v, e)| {
            (
                self.replay.edits.values[v].0,
                t,
                self.replay.edits.edits[e].1.clone(),
            )
        })
    }

    fn buffer(&self) -> &[(&'storage Row, T, Diff)] {
        &self.replay.buffer
    }

    fn step(&mut self) {
        let (time, _, value_index, edit_offset) = self.replay.history.pop().unwrap();
        self.replay.buffer.push((
            self.replay.edits.values[value_index].0,
            time,
            self.replay.edits.edits[edit_offset].1.clone(),
        ));
    }

    fn advance_buffer_by(&mut self, meet: &T) {
        for (_, time, _) in self.replay.buffer.iter_mut() {
            *time = time.join(meet);
        }
        consolidate_updates(&mut self.replay.buffer);
    }

    fn is_done(&self) -> bool {
        self.replay.history.len() == 0
    }
}
