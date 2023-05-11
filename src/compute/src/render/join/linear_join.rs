// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Rendering of linear join plans.
//!
//! Consult [LinearJoinPlan] documentation for details.

use std::cmp::Ordering;
use std::collections::VecDeque;

use differential_dataflow::difference::Multiply;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::arrangement::{Arrange, Arranged};
use differential_dataflow::trace::{BatchReader, Cursor, TraceReader};
use differential_dataflow::{AsCollection, Collection, Data};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::generic::OutputHandle;
use timely::dataflow::operators::{Capability, OkErr, Operator};
use timely::dataflow::Scope;
use timely::progress::{timestamp::Refines, Timestamp};
use timely::scheduling::Activator;
use timely::PartialOrder;

use mz_compute_client::plan::join::linear_join::{LinearJoinPlan, LinearStagePlan};
use mz_compute_client::plan::join::JoinClosure;
use mz_repr::{DatumVec, Diff, Row, RowArena};
use mz_storage_client::types::errors::DataflowError;
use mz_timely_util::operator::CollectionExt;

use crate::render::context::{
    Arrangement, ArrangementFlavor, ArrangementImport, CollectionBundle, Context,
};
use crate::typedefs::RowSpine;

/// Different forms the streamed data might take.
enum JoinedFlavor<G, T>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T>,
    T: Timestamp + Lattice,
{
    /// Streamed data as a collection.
    Collection(Collection<G, Row, Diff>),
    /// A dataflow-local arrangement.
    Local(Arrangement<G, Row>),
    /// An imported arrangement.
    Trace(ArrangementImport<G, Row, T>),
}

impl<G, T> Context<G, Row, T>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T>,
    T: Timestamp + Lattice,
{
    pub(crate) fn render_join(
        &mut self,
        inputs: Vec<CollectionBundle<G, Row, T>>,
        linear_plan: LinearJoinPlan,
    ) -> CollectionBundle<G, Row, T> {
        self.scope.region_named("Join(Linear)", |inner| {
            // Collect all error streams, and concatenate them at the end.
            let mut errors = Vec::new();

            // Determine which form our maintained spine of updates will initially take.
            // First, just check out the availability of an appropriate arrangement.
            // This will be `None` in the degenerate single-input join case, which ensures
            // that we do not panic if we never go around the `stage_plans` loop.
            let arrangement = linear_plan.stage_plans.get(0).and_then(|stage| {
                inputs[linear_plan.source_relation].arrangement(&stage.stream_key)
            });
            // We can use an arrangement if it exists and an initial closure does not.
            let mut joined = match (arrangement, linear_plan.initial_closure) {
                (Some(ArrangementFlavor::Local(oks, errs)), None) => {
                    errors.push(errs.as_collection(|k, _v| k.clone()).enter_region(inner));
                    JoinedFlavor::Local(oks.enter_region(inner))
                }
                (Some(ArrangementFlavor::Trace(_gid, oks, errs)), None) => {
                    errors.push(errs.as_collection(|k, _v| k.clone()).enter_region(inner));
                    JoinedFlavor::Trace(oks.enter_region(inner))
                }
                (_, initial_closure) => {
                    // TODO: extract closure from the first stage in the join plan, should it exist.
                    // TODO: apply that closure in `flat_map_ref` rather than calling `.collection`.
                    let (joined, errs) = inputs[linear_plan.source_relation]
                        .as_specific_collection(linear_plan.source_key.as_deref());
                    errors.push(errs.enter_region(inner));
                    let mut joined = joined.enter_region(inner);

                    // In the current code this should always be `None`, but we have this here should
                    // we change that and want to know what we should be doing.
                    if let Some(closure) = initial_closure {
                        // If there is no starting arrangement, then we can run filters
                        // directly on the starting collection.
                        // If there is only one input, we are done joining, so run filters
                        let (j, errs) = joined.flat_map_fallible("LinearJoinInitialization", {
                            // Reuseable allocation for unpacking.
                            let mut datums = DatumVec::new();
                            let mut row_builder = Row::default();
                            move |row| {
                                let temp_storage = RowArena::new();
                                let mut datums_local = datums.borrow_with(&row);
                                // TODO(mcsherry): re-use `row` allocation.
                                closure
                                    .apply(&mut datums_local, &temp_storage, &mut row_builder)
                                    .map_err(DataflowError::from)
                                    .transpose()
                            }
                        });
                        joined = j;
                        errors.push(errs);
                    }

                    JoinedFlavor::Collection(joined)
                }
            };

            // progress through stages, updating partial results and errors.
            for stage_plan in linear_plan.stage_plans.into_iter() {
                // Different variants of `joined` implement this differently,
                // and the logic is centralized there.
                let stream = differential_join(
                    joined,
                    inputs[stage_plan.lookup_relation].enter_region(inner),
                    stage_plan,
                    &mut errors,
                );
                // Update joined results and capture any errors.
                joined = JoinedFlavor::Collection(stream);
            }

            // We have completed the join building, but may have work remaining.
            // For example, we may have expressions not pushed down (e.g. literals)
            // and projections that could not be applied (e.g. column repetition).
            if let JoinedFlavor::Collection(mut joined) = joined {
                if let Some(closure) = linear_plan.final_closure {
                    let (updates, errs) = joined.flat_map_fallible("LinearJoinFinalization", {
                        // Reuseable allocation for unpacking.
                        let mut datums = DatumVec::new();
                        let mut row_builder = Row::default();
                        move |row| {
                            let temp_storage = RowArena::new();
                            let mut datums_local = datums.borrow_with(&row);
                            // TODO(mcsherry): re-use `row` allocation.
                            closure
                                .apply(&mut datums_local, &temp_storage, &mut row_builder)
                                .map_err(DataflowError::from)
                                .transpose()
                        }
                    });

                    joined = updates;
                    errors.push(errs);
                }

                // Return joined results and all produced errors collected together.
                CollectionBundle::from_collections(
                    joined,
                    differential_dataflow::collection::concatenate(inner, errors),
                )
            } else {
                panic!("Unexpectedly arranged join output");
            }
            .leave_region()
        })
    }
}

/// Looks up the arrangement for the next input and joins it to the arranged
/// version of the join of previous inputs. This is split into its own method
/// to enable reuse of code with different types of `prev_keyed`.
fn differential_join<G, T>(
    mut joined: JoinedFlavor<G, T>,
    lookup_relation: CollectionBundle<G, Row, T>,
    LinearStagePlan {
        stream_key,
        stream_thinning,
        lookup_key,
        closure,
        lookup_relation: _,
    }: LinearStagePlan,
    errors: &mut Vec<Collection<G, DataflowError, Diff>>,
) -> Collection<G, Row, Diff>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T>,
    T: Timestamp + Lattice,
{
    // If we have only a streamed collection, we must first form an arrangement.
    if let JoinedFlavor::Collection(stream) = joined {
        let mut row_buf = Row::default();
        let (keyed, errs) = stream.map_fallible("LinearJoinKeyPreparation", {
            // Reuseable allocation for unpacking.
            let mut datums = DatumVec::new();
            move |row| {
                let temp_storage = RowArena::new();
                let datums_local = datums.borrow_with(&row);
                row_buf.packer().try_extend(
                    stream_key
                        .iter()
                        .map(|e| e.eval(&datums_local, &temp_storage)),
                )?;
                let key = row_buf.clone();
                row_buf
                    .packer()
                    .extend(stream_thinning.iter().map(|e| datums_local[*e]));
                let value = row_buf.clone();
                Ok((key, value))
            }
        });

        errors.push(errs);
        let arranged = keyed.arrange_named::<RowSpine<_, _, _, _>>("JoinStage");
        joined = JoinedFlavor::Local(arranged);
    }

    // Demultiplex the four different cross products of arrangement types we might have.
    let arrangement = lookup_relation
        .arrangement(&lookup_key[..])
        .expect("Arrangement absent despite explicit construction");
    match joined {
        JoinedFlavor::Collection(_) => {
            unreachable!("JoinedFlavor::Collection variant avoided at top of method");
        }
        JoinedFlavor::Local(local) => match arrangement {
            ArrangementFlavor::Local(oks, errs1) => {
                let (oks, errs2) = differential_join_inner(local, oks, closure);
                errors.push(errs1.as_collection(|k, _v| k.clone()));
                errors.extend(errs2);
                oks
            }
            ArrangementFlavor::Trace(_gid, oks, errs1) => {
                let (oks, errs2) = differential_join_inner(local, oks, closure);
                errors.push(errs1.as_collection(|k, _v| k.clone()));
                errors.extend(errs2);
                oks
            }
        },
        JoinedFlavor::Trace(trace) => match arrangement {
            ArrangementFlavor::Local(oks, errs1) => {
                let (oks, errs2) = differential_join_inner(trace, oks, closure);
                errors.push(errs1.as_collection(|k, _v| k.clone()));
                errors.extend(errs2);
                oks
            }
            ArrangementFlavor::Trace(_gid, oks, errs1) => {
                let (oks, errs2) = differential_join_inner(trace, oks, closure);
                errors.push(errs1.as_collection(|k, _v| k.clone()));
                errors.extend(errs2);
                oks
            }
        },
    }
}

/// Joins the arrangement for `next_input` to the arranged version of the
/// join of previous inputs. This is split into its own method to enable
/// reuse of code with different types of `next_input`.
///
/// The return type includes an optional error collection, which may be
/// `None` if we can determine that `closure` cannot error.
fn differential_join_inner<G, T, Tr1, Tr2>(
    prev_keyed: Arranged<G, Tr1>,
    next_input: Arranged<G, Tr2>,
    closure: JoinClosure,
) -> (
    Collection<G, Row, Diff>,
    Option<Collection<G, DataflowError, Diff>>,
)
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T>,
    T: Timestamp + Lattice,
    Tr1: TraceReader<Key = Row, Val = Row, Time = G::Timestamp, R = Diff> + Clone + 'static,
    Tr2: TraceReader<Key = Row, Val = Row, Time = G::Timestamp, R = Diff> + Clone + 'static,
{
    // Reuseable allocation for unpacking.
    let mut datums = DatumVec::new();
    let mut row_builder = Row::default();

    if closure.could_error() {
        let (oks, err) = join_core(&prev_keyed, &next_input, move |key, old, new| {
            let temp_storage = RowArena::new();
            let mut datums_local = datums.borrow_with_many(&[key, old, new]);
            closure
                .apply(&mut datums_local, &temp_storage, &mut row_builder)
                .map_err(DataflowError::from)
                .transpose()
        })
        .inner
        .ok_err(|(x, t, d)| {
            // TODO(mcsherry): consider `ok_err()` for `Collection`.
            match x {
                Ok(x) => Ok((x, t, d)),
                Err(x) => Err((x, t, d)),
            }
        });

        (oks.as_collection(), Some(err.as_collection()))
    } else {
        let oks = join_core(&prev_keyed, &next_input, move |key, old, new| {
            let temp_storage = RowArena::new();
            let mut datums_local = datums.borrow_with_many(&[key, old, new]);
            closure
                .apply(&mut datums_local, &temp_storage, &mut row_builder)
                .expect("Closure claimed to never error")
        });

        (oks, None)
    }
}

/// Joins two arranged collections with the same key type.
///
/// Each matching pair of records `(key, val1)` and `(key, val2)` are subjected to the `result` function,
/// which produces something implementing `IntoIterator`, where the output collection will have an entry for
/// every value returned by the iterator.
fn join_core<G, Tr1, Tr2, L, I>(
    arranged1: &Arranged<G, Tr1>,
    arranged2: &Arranged<G, Tr2>,
    mut result: L,
) -> Collection<G, I::Item, Diff>
where
    G: Scope,
    G::Timestamp: Lattice,
    Tr1: TraceReader<Key = Row, Val = Row, Time = G::Timestamp, R = Diff> + Clone + 'static,
    Tr2: TraceReader<Key = Row, Val = Row, Time = G::Timestamp, R = Diff> + Clone + 'static,
    L: FnMut(&Tr1::Key, &Tr1::Val, &Tr2::Val) -> I + 'static,
    I: IntoIterator,
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
                let mut todo1 = VecDeque::new();
                let mut todo2 = VecDeque::new();

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
                    todo2.push_back(Deferred::new(
                        trace1_cursor,
                        trace1_storage,
                        batch2_cursor,
                        batch2.clone(),
                        capability.clone(),
                    ));
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
                        // This test *should* always pass, as we only drop a trace in response to the other input emptying.
                        if let Some(ref mut trace2) = trace2_option {
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
                                        let batch1_cursor = batch1.cursor();
                                        todo1.push_back(Deferred::new(
                                            trace2_cursor,
                                            trace2_storage,
                                            batch1_cursor,
                                            batch1.clone(),
                                            capability.clone(),
                                        ));
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
                        } else {
                            panic!("`trace2_option` dropped before `input1` emptied!");
                        }
                    });

                    // Drain input 2, prepare work.
                    input2.for_each(|capability, data| {
                        // This test *should* always pass, as we only drop a trace in response to the other input emptying.
                        if let Some(ref mut trace1) = trace1_option {
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
                                        let batch2_cursor = batch2.cursor();
                                        todo2.push_back(Deferred::new(
                                            trace1_cursor,
                                            trace1_storage,
                                            batch2_cursor,
                                            batch2.clone(),
                                            capability.clone(),
                                        ));
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
                        } else {
                            panic!("`trace1_option` dropped before `input2` emptied!");
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

                    let mut work_result = |k: &Tr1::Key,
                                           v1: &Tr1::Val,
                                           v2: &Tr2::Val,
                                           t: &G::Timestamp,
                                           r1: &Tr1::R,
                                           r2: &Tr2::R| {
                        let t = t.clone();
                        let r = r1.clone().multiply(r2);
                        result(k, v1, v2)
                            .into_iter()
                            .map(move |d| (d, t.clone(), r.clone()))
                    };

                    // Perform some amount of outstanding work.
                    let mut fuel = 1_000;
                    while !todo1.is_empty() && fuel > 0 {
                        todo1.front_mut().unwrap().work(
                            output,
                            |k, v2, v1, t, r2, r1| work_result(k, v1, v2, t, r1, r2),
                            &mut fuel,
                        );
                        if !todo1.front().unwrap().work_remains() {
                            todo1.pop_front();
                        }
                    }

                    // Perform some amount of outstanding work.
                    let mut fuel = 1_000;
                    while !todo2.is_empty() && fuel > 0 {
                        todo2
                            .front_mut()
                            .unwrap()
                            .work(output, &mut work_result, &mut fuel);
                        if !todo2.front().unwrap().work_remains() {
                            todo2.pop_front();
                        }
                    }

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

/// Deferred join computation.
///
/// The structure wraps cursors which allow us to play out join computation at whatever rate we like.
/// This allows us to avoid producing and buffering massive amounts of data, without giving the timely
/// dataflow system a chance to run operators that can consume and aggregate the data.
struct Deferred<T, C1, C2, D>
where
    T: Timestamp,
    C1: Cursor<Key = Row, Val = Row, Time = T, R = Diff>,
    C2: Cursor<Key = Row, Val = Row, Time = T, R = Diff>,
{
    trace: C1,
    trace_storage: C1::Storage,
    batch: C2,
    batch_storage: C2::Storage,
    capability: Capability<T>,
    done: bool,
    temp: Vec<((D, T), Diff)>,
}

impl<T, C1, C2, D> Deferred<T, C1, C2, D>
where
    T: Timestamp + Lattice,
    C1: Cursor<Key = Row, Val = Row, Time = T, R = Diff>,
    C2: Cursor<Key = Row, Val = Row, Time = T, R = Diff>,
    D: Data,
{
    fn new(
        trace: C1,
        trace_storage: C1::Storage,
        batch: C2,
        batch_storage: C2::Storage,
        capability: Capability<T>,
    ) -> Self {
        Deferred {
            trace,
            trace_storage,
            batch,
            batch_storage,
            capability,
            done: false,
            temp: Vec::new(),
        }
    }

    fn work_remains(&self) -> bool {
        !self.done
    }

    /// Process keys until at least `fuel` output tuples produced, or the work is exhausted.
    #[inline(never)]
    fn work<L, I>(
        &mut self,
        output: &mut OutputHandle<T, (D, T, Diff), Tee<T, (D, T, Diff)>>,
        mut logic: L,
        fuel: &mut usize,
    ) where
        I: IntoIterator<Item = (D, T, Diff)>,
        L: FnMut(&C1::Key, &C1::Val, &C2::Val, &T, &C1::R, &C2::R) -> I,
    {
        let meet = self.capability.time();

        let mut session = output.session(&self.capability);

        let trace_storage = &self.trace_storage;
        let batch_storage = &self.batch_storage;

        let trace = &mut self.trace;
        let batch = &mut self.batch;

        let temp = &mut self.temp;

        while batch.key_valid(batch_storage) && trace.key_valid(trace_storage) {
            match trace.key(trace_storage).cmp(batch.key(batch_storage)) {
                Ordering::Less => trace.seek_key(trace_storage, batch.key(batch_storage)),
                Ordering::Greater => batch.seek_key(batch_storage, trace.key(trace_storage)),
                Ordering::Equal => {
                    assert_eq!(temp.len(), 0);

                    // Populate `temp` with the results, as long as fuel remains.
                    let key = batch.key(batch_storage);
                    while let Some(val1) = trace.get_val(trace_storage) {
                        while let Some(val2) = batch.get_val(batch_storage) {
                            trace.map_times(trace_storage, |time1, diff1| {
                                let time1 = time1.join(meet);
                                batch.map_times(batch_storage, |time2, diff2| {
                                    let time = time1.join(time2);
                                    for (d, t, r) in logic(key, val1, val2, &time, diff1, diff2) {
                                        temp.push(((d, t), r));
                                    }
                                });
                            });
                            batch.step_val(batch_storage);
                        }
                        batch.rewind_vals(batch_storage);
                        trace.step_val(trace_storage);

                        // TODO: This consolidation is optional, and it may not be very
                        //       helpful. We might try harder to understand whether we
                        //       should do this work here, or downstream at consumers.
                        // TODO: Perhaps `thinker` should have the buffer, do smarter
                        //       consolidation, and then deposit results in `session`.
                        differential_dataflow::consolidation::consolidate(temp);

                        *fuel = fuel.saturating_sub(temp.len());
                        for ((d, t), r) in temp.drain(..) {
                            session.give((d, t, r));
                        }

                        if *fuel == 0 {
                            // The fuel is exhausted, so we should yield. Returning here is only
                            // allowed because we leave the cursors in a state that will let us
                            // pick up the work correctly on the next invocation.
                            return;
                        }
                    }

                    batch.step_key(batch_storage);
                    trace.step_key(trace_storage);
                }
            }
        }

        // We only get here after having iterated through all keys.
        self.done = true;
    }
}
