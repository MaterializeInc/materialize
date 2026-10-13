//! Match pairs of records based on a key.
//!
//! The various `join` implementations require that the units of each collection can be multiplied, and that
//! the multiplication distributes over addition. That is, we will repeatedly evaluate (a + b) * c as (a * c)
//! + (b * c), and if this is not equal to the former term, little is known about the actual output.
use std::cmp::Ordering;
use std::collections::VecDeque;

use timely::{Container, ContainerBuilder};
use timely::container::NoopBuilder;
use timely::order::PartialOrder;
use timely::progress::Timestamp;
use timely::dataflow::Stream;
use timely::dataflow::operators::generic::{Operator, OutputBuilderSession};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Capability;

use crate::lattice::Lattice;
use crate::operators::arrange::Arranged;
use crate::trace::{BatchCursor, BatchDiff, BatchReader, BatchVal, Cursor, Navigable, TraceReader};
use crate::trace::cursor::cursor_list;
use crate::trace::implementations::containers::BatchContainer;
use crate::operators::ValueHistory;

/// A type that can manage the joining of lists of batches.
///
/// The trait is parameterized by the output container `C`, not by the builder that assembles it: a tactic
/// yields finished containers, and how it produces them (pushing records into a [`ContainerBuilder`], or
/// otherwise) is its own concern.
pub trait JoinTactic<B0: BatchReader, B1: BatchReader<Time = B0::Time>, C> {
    /// Prepare the join of two lists of batches into an iterator of output containers.
    ///
    /// The supplied `fresh` and `meet` indicate respectively which input is "novel", and should drive the
    /// join, as well as a lower bound on that input's times, so that the other input can be loaded compacted.
    fn prep(&mut self, input0: Vec<B0>, input1: Vec<B1>, fresh: Fresh, meet: B0::Time) -> Box<dyn Iterator<Item = C>>;
}

/// Which input contributed the freshly-arrived batch of a deferred join unit.
///
/// The fresh batch's times all lie at or beyond the capability, so its side is not advanced by the
/// capability's meet; the opposing accumulated trace is. The marker also selects which queue a unit
/// joins, so a burst on one input cannot starve the other.
pub enum Fresh {
    /// The first input (`B0`) contributed the fresh batch.
    Input0,
    /// The second input (`B1`) contributed the fresh batch.
    Input1,
}

/// An equijoin of two traces, sharing a common key type.
///
/// This method exists to provide join functionality without opinions on the specific input types, keys and values,
/// that should be presented. The two traces here can have arbitrary key and value types, which can be unsized and
/// even potentially unrelated to the input collection data. Importantly, the key and value types could be generic
/// associated types (GATs) of the traces, and we would seemingly struggle to frame these types as trait arguments.
///
/// The implementation produces a caller-specified container. Implementations can use [`AsCollection`] to wrap the
/// output stream in a collection.
///
/// The "correctness" of this method depends heavily on the behavior of the supplied `result` function.
///
/// [`AsCollection`]: crate::collection::AsCollection
pub fn join_traces<'scope, Tr1, Tr2, KC, L, CB>(arranged1: Arranged<'scope, Tr1>, arranged2: Arranged<'scope, Tr2>, result: L) -> Stream<'scope, Tr1::Time, CB::Container>
where
    Tr1: TraceReader<Batch: Navigable>+'static,
    Tr2: TraceReader<Batch: Navigable, Time = Tr1::Time>+'static,
    KC: BatchContainer,
    BatchCursor<Tr1>: Cursor<Time = Tr1::Time, KeyContainer = KC>,
    for<'a> BatchCursor<Tr1>: Cursor<Key<'a> = KC::ReadItem<'a>>,
    for<'a> BatchCursor<Tr2>: Cursor<Key<'a> = KC::ReadItem<'a>, Time = Tr1::Time>,
    L: FnMut(KC::ReadItem<'_>,BatchVal<'_, Tr1>,BatchVal<'_, Tr2>,Tr1::Time,&BatchDiff<Tr1>,&BatchDiff<Tr2>,&mut CB)+'static,
    CB: ContainerBuilder<Container: Default> + 'static,
{
    join_with_tactic(arranged1, arranged2, cursors::CursorTactic::<Tr1::Batch, Tr2::Batch, _, CB>::new(result))
}

/// Drives an equijoin of two traces using a supplied [`JoinTactic`].
///
/// This is the general join operator: it does the dataflow plumbing (frontiers, capabilities, trace
/// compaction) and routes the per-batch work through the tactic. It requires only `TraceReader` of its
/// inputs, never `Navigable`: it extracts trace batches via `batches_through`, and building cursors over
/// them (if that is how the join proceeds) is the tactic's concern.
pub fn join_with_tactic<'scope, Tr1, Tr2, T, C>(arranged1: Arranged<'scope, Tr1>, arranged2: Arranged<'scope, Tr2>, mut tactic: T) -> Stream<'scope, Tr1::Time, C>
where
    Tr1: TraceReader+'static,
    Tr2: TraceReader<Time = Tr1::Time>+'static,
    T: JoinTactic<Tr1::Batch, Tr2::Batch, C>+'static,
    C: Container + 'static,
{
    // Rename traces for symmetry from here on out.
    let mut trace1 = arranged1.trace;
    let mut trace2 = arranged2.trace;

    let scope = arranged1.stream.scope();
    arranged1.stream.binary_frontier(arranged2.stream, Pipeline, Pipeline, "Join", move |capability, info| {

        // Acquire an activator to reschedule the operator when it has unfinished work.
        use timely::scheduling::Activator;
        let activations = scope.activations();
        let activator = Activator::new(info.address, activations);

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
        let mut acknowledged1 = Antichain::from_elem(Tr1::Time::minimum());
        let mut acknowledged2 = Antichain::from_elem(Tr1::Time::minimum());

        // Deferred work, as `(capability, iterator)` pairs bucketed by which input carried the fresh
        // batch (so a burst on one input cannot starve the other). The driver owns the capabilities and
        // the fuel budget; each iterator, prepared by the tactic, yields the output containers to ship
        // under its paired capability, and is dropped once it goes dry.
        let mut todo0: VecDeque<(Capability<Tr1::Time>, Box<dyn Iterator<Item = C>>)> = VecDeque::new();
        let mut todo1: VecDeque<(Capability<Tr1::Time>, Box<dyn Iterator<Item = C>>)> = VecDeque::new();

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
        assert!(PartialOrder::less_equal(&trace1.get_physical_compaction(), &acknowledged1.borrow()));

        // We capture batch2's batches first and establish work second to avoid taking a `RefCell` lock
        // on both traces at the same time, as they could be the same trace and this would panic.
        let mut batch2_list = Vec::new();
        trace2.map_batches(|batch2| {
            acknowledged2.clone_from(batch2.upper());
            batch2_list.push(batch2.clone());
        });
        // At this point, `ack2` should exactly equal `trace2.read_upper()`, as they are both determined by
        // iterating through batches and capturing the upper bound. This is a great moment to assert that
        // `trace2`'s physical compaction frontier is before the frontier of completed times in `trace2`.
        // TODO: in the case that this does not hold, instead "upgrade" the physical compaction frontier.
        assert!(PartialOrder::less_equal(&trace2.get_physical_compaction(), &acknowledged2.borrow()));

        // Batches wholly at or before these frontiers were joined by the start-up loading
        // above; batches arriving on the input streams are ignored up to them. Beyond them,
        // every non-empty arriving batch must be joined, even when `acknowledged` has been
        // advanced past it: `advance_upper` consults the shared trace, whose merges may have
        // consolidated an in-flight batch's updates away (e.g. an add/remove pair collapsing
        // once logical compaction equates their times). The trace's emptiness there is valid
        // only for readers at or beyond the compaction frontier, while our consumers may read
        // finer times; the raw batch still owes them its updates. (#801)
        let preload_upper1 = acknowledged1.clone();
        let preload_upper2 = acknowledged2.clone();

        // Load up deferred work joining each captured `trace2` batch against `trace1`.
        for batch2 in batch2_list.into_iter() {
            // It is safe to ask for `ack1` because we have confirmed it to be in advance of `distinguish_since`.
            let trace1_storage = trace1.batches_through(acknowledged1.borrow()).unwrap();
            // We could downgrade the capability here, but doing so is a bit complicated mathematically.
            // TODO: downgrade the capability by searching out the one time in `batch2.lower()` and not
            // in `batch2.upper()`. Only necessary for non-empty batches, as empty batches may not have
            // that property.
            let work = tactic.prep(trace1_storage, vec![batch2], Fresh::Input1, capability.time().clone());
            todo1.push_back((capability.clone(), work));
        }

        // Droppable handles to shared trace data structures.
        let mut trace1_option = Some(trace1);
        let mut trace2_option = Some(trace2);

        move |(input1, frontier1), (input2, frontier2), output| {

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
                // This test *should* always pass, as we only drop a trace in response to the other input emptying.
                if let Some(ref mut trace2) = trace2_option {
                    let capability = capability.retain(0);
                    for batch1 in data.drain(..) {
                        // An arriving batch must lie wholly on one side of the preload boundary,
                        // and wholly on one side of `acknowledged1`: both frontiers are drawn from
                        // the lattice of stream batch boundaries (received uppers, and uppers of
                        // trace merges of whole stream batches). A batch spanning the former would
                        // be partially double-processed; one spanning the latter mis-accounted.
                        assert!(
                            PartialOrder::less_equal(batch1.upper(), &preload_upper1) ||
                            PartialOrder::less_equal(&preload_upper1, batch1.lower()),
                            "batch spans the preload boundary",
                        );
                        assert!(
                            PartialOrder::less_equal(&acknowledged1, batch1.lower()) ||
                            PartialOrder::less_equal(batch1.upper(), &acknowledged1),
                            "batch spans the acknowledged frontier",
                        );

                        // Ignore any pre-loaded data, which was joined at start-up. Note that this
                        // is a test against the preload boundary, not against `acknowledged1`: the
                        // latter can be advanced past an in-flight batch by `advance_upper`, when
                        // trace merges consolidate the batch's updates away, and such a batch must
                        // still be joined (its updates remain real at times finer than the trace's
                        // compaction frontier, and no other work item has accounted for them).
                        if !PartialOrder::less_equal(batch1.upper(), &preload_upper1) {
                            if !batch1.is_empty() {
                                // It is safe to ask for `ack2` as we validated that it was at least `get_physical_compaction()`
                                // at start-up, and have held back physical compaction ever since.
                                let trace2_storage = trace2.batches_through(acknowledged2.borrow()).unwrap();
                                let work = tactic.prep(vec![batch1.clone()], trace2_storage, Fresh::Input0, capability.time().clone());
                                todo0.push_back((capability.clone(), work));
                            }

                            // To update `acknowledged1` we might presume that `batch1.lower` should equal it, but we
                            // may have skipped over empty batches. Still, the batches are in-order, and we should be
                            // able to just assume the most recent `batch1.upper`, unless `advance_upper` has already
                            // moved `acknowledged1` past this batch, in which case we keep the further frontier.
                            if PartialOrder::less_equal(&acknowledged1, batch1.lower()) {
                                debug_assert!(PartialOrder::less_equal(&acknowledged1, batch1.upper()));
                                acknowledged1.clone_from(batch1.upper());
                            }
                        }
                    }
                }
                else { panic!("`trace2_option` dropped before `input1` emptied!"); }
            });

            // Drain input 2, prepare work.
            input2.for_each(|capability, data| {
                // This test *should* always pass, as we only drop a trace in response to the other input emptying.
                if let Some(ref mut trace1) = trace1_option {
                    let capability = capability.retain(0);
                    for batch2 in data.drain(..) {
                        // An arriving batch must lie wholly on one side of the preload boundary,
                        // and wholly on one side of `acknowledged2`: both frontiers are drawn from
                        // the lattice of stream batch boundaries (received uppers, and uppers of
                        // trace merges of whole stream batches). A batch spanning the former would
                        // be partially double-processed; one spanning the latter mis-accounted.
                        assert!(
                            PartialOrder::less_equal(batch2.upper(), &preload_upper2) ||
                            PartialOrder::less_equal(&preload_upper2, batch2.lower()),
                            "batch spans the preload boundary",
                        );
                        assert!(
                            PartialOrder::less_equal(&acknowledged2, batch2.lower()) ||
                            PartialOrder::less_equal(batch2.upper(), &acknowledged2),
                            "batch spans the acknowledged frontier",
                        );

                        // Ignore any pre-loaded data, which was joined at start-up. Note that this
                        // is a test against the preload boundary, not against `acknowledged2`: the
                        // latter can be advanced past an in-flight batch by `advance_upper`, when
                        // trace merges consolidate the batch's updates away, and such a batch must
                        // still be joined (its updates remain real at times finer than the trace's
                        // compaction frontier, and no other work item has accounted for them).
                        if !PartialOrder::less_equal(batch2.upper(), &preload_upper2) {
                            if !batch2.is_empty() {
                                // It is safe to ask for `ack1` as we validated that it was at least `get_physical_compaction()`
                                // at start-up, and have held back physical compaction ever since.
                                let trace1_storage = trace1.batches_through(acknowledged1.borrow()).unwrap();
                                let work = tactic.prep(trace1_storage, vec![batch2.clone()], Fresh::Input1, capability.time().clone());
                                todo1.push_back((capability.clone(), work));
                            }

                            // To update `acknowledged2` we might presume that `batch2.lower` should equal it, but we
                            // may have skipped over empty batches. Still, the batches are in-order, and we should be
                            // able to just assume the most recent `batch2.upper`, unless `advance_upper` has already
                            // moved `acknowledged2` past this batch, in which case we keep the further frontier.
                            if PartialOrder::less_equal(&acknowledged2, batch2.lower()) {
                                debug_assert!(PartialOrder::less_equal(&acknowledged2, batch2.upper()));
                                acknowledged2.clone_from(batch2.upper());
                            }
                        }
                    }
                }
                else { panic!("`trace1_option` dropped before `input2` emptied!"); }
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

            // Perform some amount of outstanding work by pulling the deferred iterators and shipping the
            // containers they yield. Each direction drains against its own half of the budget, so a burst
            // on one input cannot starve the other. We reschedule the operator whenever any work remains,
            // which is observable directly: an iterator has yet to yield `None`. The budget is split from
            // `2_000_000` to preserve the historical `1_000_000` of progress per input each activation.
            // The driver only ships finished containers (`give_container`), never pushing records, so it
            // pins the operator output to `NoopBuilder<C>` — the builder for exactly this "containers ready
            // to go" case, which is a `ContainerBuilder` for any `C` without further bounds.
            let output: &mut OutputBuilderSession<'_, Tr1::Time, NoopBuilder<C>> = output;
            let mut drain = |queue: &mut VecDeque<(Capability<Tr1::Time>, Box<dyn Iterator<Item = C>>)>, mut fuel: isize| {
                while fuel >= 0 {
                    let Some((capability, work)) = queue.front_mut() else { break };
                    match work.next() {
                        Some(mut container) => {
                            fuel -= container.record_count() as isize;
                            output.session_with_builder(&*capability).give_container(&mut container);
                        }
                        None => { queue.pop_front(); }
                    }
                }
            };
            let fuel = 2_000_000;
            drain(&mut todo0, fuel / 2);
            drain(&mut todo1, fuel / 2);
            if !todo0.is_empty() || !todo1.is_empty() {
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
                if frontier2.is_empty() { trace1_option = None; }
                else {
                    // Allow `trace1` to compact logically up to the frontier we may yet receive,
                    // in the opposing input (`input2`). All `input2` times will be beyond this
                    // frontier, and joined times only need to be accurate when advanced to it.
                    trace1.set_logical_compaction(frontier2.frontier());
                    // Allow `trace1` to compact physically up to the upper bound of batches we
                    // have received in its input (`input1`). We will not require a cursor that
                    // is not beyond this bound.
                    trace1.set_physical_compaction(acknowledged1.borrow());
                }
            }

            // Maintain `trace2`. Drop if `input1` is empty, or advance based on future needs.
            if let Some(trace2) = trace2_option.as_mut() {
                if frontier1.is_empty() { trace2_option = None;}
                else {
                    // Allow `trace2` to compact logically up to the frontier we may yet receive,
                    // in the opposing input (`input1`). All `input1` times will be beyond this
                    // frontier, and joined times only need to be accurate when advanced to it.
                    trace2.set_logical_compaction(frontier1.frontier());
                    // Allow `trace2` to compact physically up to the upper bound of batches we
                    // have received in its input (`input2`). We will not require a cursor that
                    // is not beyond this bound.
                    trace2.set_physical_compaction(acknowledged2.borrow());
                }
            }
        }
    })
}

/// Cursor-based join: the conventional [`JoinTactic`] implementation and its per-batch worker.
mod cursors {

    use std::cell::RefCell;
    use std::rc::Rc;

    use super::*;

    /// The conventional cursor-based [`JoinTactic`].
    ///
    /// It builds a [`CursorList`] over each input batch list and plays the merge-join out at whatever rate
    /// the driver's fuel allows. Each prepared unit joins a `B0`-side cursor against a `B1`-side cursor,
    /// emitting `(val0, val1)` to `logic` and yielding the output containers `logic` fills. `logic` is
    /// shared across all outstanding units (an `Rc<RefCell<_>>`), preserving the single mutable-state
    /// semantics of one closure threaded through every match — each unit is a self-contained `'static`
    /// iterator, so it cannot borrow the tactic.
    ///
    /// It is parameterized by the builder `CB` into which `logic` pushes output; the [`JoinTactic`] it
    /// implements is over the container `CB` yields (`CB::Container`).
    pub struct CursorTactic<B0, B1, L, CB>
    where
        B0: BatchReader + Navigable,
        B1: BatchReader<Time = B0::Time> + Navigable,
        B0::Cursor: Cursor<Time = B0::Time>,
        B1::Cursor: for<'a> Cursor<Key<'a> = <B0::Cursor as Cursor>::Key<'a>, Time = B0::Time>,
    {
        logic: Rc<RefCell<L>>,
        _marker: std::marker::PhantomData<(B0, B1, CB)>,
    }

    impl<B0, B1, L, CB> CursorTactic<B0, B1, L, CB>
    where
        B0: BatchReader + Navigable,
        B1: BatchReader<Time = B0::Time> + Navigable,
        B0::Cursor: Cursor<Time = B0::Time>,
        B1::Cursor: for<'a> Cursor<Key<'a> = <B0::Cursor as Cursor>::Key<'a>, Time = B0::Time>,
    {
        /// Construct a tactic that applies `logic` to each matched `(key, val0, val1)`.
        pub fn new(logic: L) -> Self {
            CursorTactic { logic: Rc::new(RefCell::new(logic)), _marker: std::marker::PhantomData }
        }
    }

    impl<B0, B1, L, CB> JoinTactic<B0, B1, CB::Container> for CursorTactic<B0, B1, L, CB>
    where
        B0: BatchReader + Navigable + 'static,
        B1: BatchReader<Time = B0::Time> + Navigable + 'static,
        B0::Cursor: Cursor<Time = B0::Time>,
        B1::Cursor: for<'a> Cursor<Key<'a> = <B0::Cursor as Cursor>::Key<'a>, Time = B0::Time>,
        CB: ContainerBuilder<Container: Default> + 'static,
        L: for<'a> FnMut(<B0::Cursor as Cursor>::Key<'a>, <B0::Cursor as Cursor>::Val<'a>, <B1::Cursor as Cursor>::Val<'a>, B0::Time, &<B0::Cursor as Cursor>::Diff, &<B1::Cursor as Cursor>::Diff, &mut CB) + 'static,
    {
        fn prep(&mut self, input0: Vec<B0>, input1: Vec<B1>, fresh: Fresh, meet: B0::Time) -> Box<dyn Iterator<Item = CB::Container>> {
            // The accumulated side's history is advanced by `meet` to consolidate it before the
            // cross-product; the fresh side is left, as its times already lie at or beyond `meet`. `fresh`
            // fixes which side is which. The advance is output-neutral either way (the fresh side's times are
            // at or beyond `meet`, so the joined time is too), so it is purely a consolidation: it pays off
            // when the accumulated side carries times below `meet`, and is a wasted scan when it does not. A
            // more precise rule would skip the scan when the side is already entirely at or beyond `meet`,
            // but detecting that needs both frontiers, not just `lower`: a batch's times lie at or beyond
            // both its `lower` and its `since`, so the side is entirely beyond `meet` exactly when
            // `meet <= lower` or `meet <= since`. A fresh batch is caught by `lower` (its `since` is
            // `minimum`), a compacted trace by `since` (its `lower` is `minimum`); checking `lower` alone
            // would wrongly advance a compacted trace whose times are all already at or beyond `meet`. We
            // keep the simpler fresh-based choice and accept the occasional no-op scan.
            let (cursor1, storage1) = cursor_list(input0);
            let (cursor2, storage2) = cursor_list(input1);
            let (advance1, advance2) = match fresh {
                Fresh::Input0 => (false, true),
                Fresh::Input1 => (true, false),
            };
            Box::new(DeferredIter {
                cursor1,
                storage1,
                cursor2,
                storage2,
                meet,
                advance1,
                advance2,
                logic: Rc::clone(&self.logic),
                builder: CB::default(),
                ready: VecDeque::new(),
                done: false,
            })
        }
    }

    /// Deferred join computation, as an iterator of output containers.
    ///
    /// The structure wraps cursors which allow us to play out join computation at whatever rate we like.
    /// This allows us to avoid producing and buffering massive amounts of data, without giving the timely
    /// dataflow system a chance to run operators that can consume and aggregate the data. Each `next` plays
    /// the merge-join forward until the builder yields a container (or the cursors run dry), matching the
    /// former per-unit `work` loop but suspending at container boundaries rather than under a fuel budget:
    /// the driver stops pulling once its budget is spent and resumes the same iterator next activation.
    struct DeferredIter<T, C1, C2, L, CB>
    where
        T: Timestamp+Lattice,
        C1: Cursor<Time=T>,
        C2: for<'a> Cursor<Key<'a>=C1::Key<'a>, Time=T>,
        CB: ContainerBuilder,
    {
        cursor1: C1,
        storage1: C1::Storage,
        cursor2: C2,
        storage2: C2::Storage,
        /// The capability's time, at which this unit's output ships; the lower envelope for consolidation.
        meet: T,
        /// Whether to advance each side's history by `meet` before consolidation.
        advance1: bool,
        advance2: bool,
        /// The output closure, shared across all outstanding units.
        logic: Rc<RefCell<L>>,
        /// The builder `logic` fills; drained into `ready` as containers complete.
        builder: CB,
        /// Completed containers awaiting a `next` call.
        ready: VecDeque<CB::Container>,
        done: bool,
    }

    impl<T, C1, C2, L, CB> Iterator for DeferredIter<T, C1, C2, L, CB>
    where
        T: Timestamp+Lattice,
        C1: Cursor<Time=T>,
        C2: for<'a> Cursor<Key<'a>=C1::Key<'a>, Time=T>,
        CB: ContainerBuilder<Container: Default>,
        L: for<'a> FnMut(C1::Key<'a>, C1::Val<'a>, C2::Val<'a>, T, &C1::Diff, &C2::Diff, &mut CB),
    {
        type Item = CB::Container;

        /// Play the merge-join forward until a container is ready, or the cursors run dry.
        #[inline(never)]
        fn next(&mut self) -> Option<CB::Container> {
            // Serve any container completed on an earlier call first.
            if let Some(container) = self.ready.pop_front() { return Some(container); }
            if self.done { return None; }

            // The accumulated side is advanced by `meet` to consolidate its history; the fresh side is left,
            // as its times already lie at or beyond `meet`. The choice was fixed per side at construction,
            // from which input carried the fresh batch.
            let meet1 = if self.advance1 { Some(&self.meet) } else { None };
            let meet2 = if self.advance2 { Some(&self.meet) } else { None };

            let storage1 = &self.storage1;
            let storage2 = &self.storage2;
            let cursor1 = &mut self.cursor1;
            let cursor2 = &mut self.cursor2;
            let builder = &mut self.builder;
            let ready = &mut self.ready;
            let mut logic = self.logic.borrow_mut();
            let logic = &mut *logic;

            let mut thinker = JoinThinker::new();
            let mut exhausted = false;

            while ready.is_empty() {
                match (cursor1.get_key(storage1), cursor2.get_key(storage2)) {
                    (Some(key1), Some(key2)) => match key1.cmp(&key2) {
                        Ordering::Less => cursor1.seek_key(storage1, key2),
                        Ordering::Greater => cursor2.seek_key(storage2, key1),
                        Ordering::Equal => {

                            thinker.history1.edits.load(cursor1, storage1, meet1);
                            thinker.history2.edits.load(cursor2, storage2, meet2);

                            thinker.think(|v1,v2,t,r1,r2| {
                                logic(key1, v1, v2, t, r1, r2, builder);
                            });

                            cursor1.step_key(storage1);
                            cursor2.step_key(storage2);

                            thinker.history1.clear();
                            thinker.history2.clear();

                            // Move any completed containers aside; we yield them one at a time.
                            while let Some(container) = builder.extract() {
                                // Avoiding the mem::take would require a non-iterator trait.
                                ready.push_back(std::mem::take(container));
                            }
                        }
                    },
                    // One side is exhausted; no further keys can match.
                    _ => { exhausted = true; break; }
                }
            }

            if exhausted {
                self.done = true;
                // Flush the final partial container.
                while let Some(container) = builder.finish() {
                    // Avoiding the mem::take would require a non-iterator trait.
                    ready.push_back(std::mem::take(container));
                }
            }

            ready.pop_front()
        }
    }

    struct JoinThinker<V1, V2, T, D1, D2> {
        pub history1: ValueHistory<V1, T, D1>,
        pub history2: ValueHistory<V2, T, D2>,
    }

    impl<V1, V2, T, D1, D2> JoinThinker<V1, V2, T, D1, D2>
    where
        V1: Copy + Ord,
        V2: Copy + Ord,
        T: Ord + Clone + Lattice,
        D1: Clone + crate::difference::Semigroup,
        D2: Clone + crate::difference::Semigroup,
    {
        fn new() -> Self {
            JoinThinker {
                history1: ValueHistory::new(),
                history2: ValueHistory::new(),
            }
        }

        fn think<F: FnMut(V1, V2, T, &D1, &D2)>(&mut self, mut results: F) {

            // for reasonably sized edits, do the dead-simple thing.
            if self.history1.edits.len() < 10 || self.history2.edits.len() < 10 {
                self.history1.edits.map(|v1, t1, d1| {
                    self.history2.edits.map(|v2, t2, d2| {
                        results(v1, v2, t1.join(t2), d1, d2);
                    })
                })
            }
            else {

                let mut replay1 = self.history1.replay();
                let mut replay2 = self.history2.replay();

                // TODO: It seems like there is probably a good deal of redundant `advance_buffer_by`
                //       in here. If a time is ever repeated, for example, the call will be identical
                //       and accomplish nothing. If only a single record has been added, it may not
                //       be worth the time to collapse (advance, re-sort) the data when a linear scan
                //       is sufficient.

                while !replay1.is_done() && !replay2.is_done() {

                    if replay1.time().unwrap().cmp(replay2.time().unwrap()) == ::std::cmp::Ordering::Less {
                        replay2.advance_buffer_by(replay1.meet().unwrap());
                        for &((val2, ref time2), ref diff2) in replay2.buffer().iter() {
                            let (val1, time1, diff1) = replay1.edit().unwrap();
                            results(val1, val2, time1.join(time2), diff1, diff2);
                        }
                        replay1.step();
                    }
                    else {
                        replay1.advance_buffer_by(replay2.meet().unwrap());
                        for &((val1, ref time1), ref diff1) in replay1.buffer().iter() {
                            let (val2, time2, diff2) = replay2.edit().unwrap();
                            results(val1, val2, time1.join(time2), diff1, diff2);
                        }
                        replay2.step();
                    }
                }

                while !replay1.is_done() {
                    replay2.advance_buffer_by(replay1.meet().unwrap());
                    for &((val2, ref time2), ref diff2) in replay2.buffer().iter() {
                        let (val1, time1, diff1) = replay1.edit().unwrap();
                        results(val1, val2, time1.join(time2), diff1, diff2);
                    }
                    replay1.step();
                }
                while !replay2.is_done() {
                    replay1.advance_buffer_by(replay2.meet().unwrap());
                    for &((val1, ref time1), ref diff1) in replay1.buffer().iter() {
                        let (val2, time2, diff2) = replay2.edit().unwrap();
                        results(val1, val2, time1.join(time2), diff1, diff2);
                    }
                    replay2.step();
                }
            }
        }
    }
}
